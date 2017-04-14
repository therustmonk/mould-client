#[macro_use] extern crate log;
#[macro_use] extern crate error_chain;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate serde_json;
extern crate websocket;

use std::fmt;
use std::str;
use std::result;
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::de::{Visitor};
use serde_json::{Map, Value};
use serde_json::value::ToJson;
use websocket::message::{Message, Type};

error_chain! {
    foreign_links {
        EncodingError(str::Utf8Error);
        SerdeError(serde_json::Error);
        WebSocketError(websocket::result::WebSocketError);
    }
    errors {
        InteractionFinished {
            description("interaction finished")
        }
        UnexpectedFormat {
            description("unexpected data format")
        }
        UnexpectedKind(s: String) {
            description("unsexpected event")
            display("unexpected event: '{}'", s)
        }
        Interrupted {
            description("connection interrupted")
        }
        ActionRejected(s: String) {
            description("action rejected")
            display("action rejected: '{}'", s)
        }
        ActionFailed(s: String) {
            description("action failed")
            display("action failed: '{}'", s)
        }
        NoDataProvided {
            description("no data provided")
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Event {
    event: EventKind,
    data: Option<Value>,
}

#[derive(Debug)]
pub enum EventKind {
    Request,
    Ready,
    Item,
    Next,
    Reject,
    Fail,
    Done,
    Cancel,
    Suspended,
}

impl Serialize for EventKind {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
        where S: Serializer
    {
        let kind = match *self {
            EventKind::Request => "request",
            EventKind::Ready => "ready",
            EventKind::Item => "item",
            EventKind::Next => "next",
            EventKind::Reject => "reject",
            EventKind::Fail => "fail",
            EventKind::Done => "done",
            EventKind::Cancel => "cancel",
            EventKind::Suspended => "suspended",
        };
        serializer.serialize_str(kind)
    }
}

impl Deserialize for EventKind {
    fn deserialize<D>(deserializer: D) -> result::Result<EventKind, D::Error>
        where D: Deserializer
    {
        struct FieldVisitor {
            min: usize,
        };

        impl Visitor for FieldVisitor {
            type Value = EventKind;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a string containing at least {} bytes", self.min)
            }

            fn visit_str<E>(self, value: &str) -> result::Result<EventKind, E>
                where E: serde::de::Error
            {
                let kind = match value {
                    "request" => EventKind::Request,
                    "ready" => EventKind::Ready,
                    "next" => EventKind::Next,
                    "reject" => EventKind::Reject,
                    "fail" => EventKind::Fail,
                    "done" => EventKind::Done,
                    "cancel" => EventKind::Cancel,
                    "suspended" => EventKind::Suspended,
                    s => {
                        return Err(serde::de::Error::invalid_value(serde::de::Unexpected::Str(s), &self));
                    },
                };
                Ok(kind)
            }
        }
        deserializer.deserialize_str(FieldVisitor { min: 4 })
    }
}


#[derive(Serialize, Deserialize)]
pub struct InteractionRequest {
    pub service: String,
    pub action: String,
    pub payload: Map<String, Value>,
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    pub action: String,
    pub payload: Map<String, Value>,
}

pub struct Client<S: websocket::Stream> {
    inner: websocket::Client<S>,
}

impl<S: websocket::Stream> From<websocket::Client<S>> for Client<S> {
    fn from(ws: websocket::Client<S>) -> Self {
        Client {
            inner: ws,
        }
    }
}

pub struct BusyClient<S: websocket::Stream> {
    finished: bool,
    inner: websocket::Client<S>,
}

impl<S> Client<S>
    where S: websocket::Stream {

    pub fn start_interaction(mut self, request: InteractionRequest) -> Result<BusyClient<S>> {
        let event = Event {
            event: EventKind::Request,
            data: Some(request.to_json()?),
        };
        let message = Message::text(serde_json::to_string(&event)?);
        self.inner.send_message(&message)?;
        let busy = BusyClient {
            finished: false,
            inner: self.inner,
        };
        Ok(busy)
    }
}

pub struct Response {
    pub items: Vec<Value>,
    pub last: bool,
}

impl<S> BusyClient<S>
    where S: websocket::Stream {

    pub fn send_next(&mut self, request: Option<Request>) -> Result<Response> {
        if self.finished {
            return Err(ErrorKind::InteractionFinished.into());
        }
        let request = match request {
            Some(request) => Some(request.to_json()?),
            None => None,
        };
        let event = Event {
            event: EventKind::Next,
            data: request,
        };
        let message = Message::text(serde_json::to_string(&event)?);
        self.inner.send_message(&message)?;
        let mut items = Vec::new();
        loop {
            let message: Message = self.inner.recv_message()?;
            match message.opcode {
                Type::Text => {
                    let content = str::from_utf8(&*message.payload)?;
                    println!("{}", content);
                    let Event { event, data } = serde_json::from_str(content)?;
                    match event {
                        EventKind::Done => {
                            self.finished = true;
                            break;
                        },
                        EventKind::Item => {
                            if let Some(data) = data {
                                items.push(data);
                            } else {
                                return Err(ErrorKind::NoDataProvided.into());
                            }
                        },
                        EventKind::Ready => {
                            break;
                        },
                        EventKind::Reject => {
                            let reason = data.as_ref().and_then(Value::as_str).unwrap_or("<no reject reason>");
                            return Err(ErrorKind::ActionRejected(reason.into()).into());
                        },
                        EventKind::Fail => {
                            let reason = data.as_ref().and_then(Value::as_str).unwrap_or("<no fail reason>");
                            return Err(ErrorKind::ActionFailed(reason.into()).into());
                        },
                        kind => {
                            return Err(ErrorKind::UnexpectedKind(format!("{:?}", kind)).into());
                        },
                    }
                },
                Type::Close => {
                    return Err(ErrorKind::Interrupted.into());
                },
                Type::Ping => {
                    self.inner.send_message(&Message::pong(message.payload))?;
                },
                Type::Pong => {
                    trace!("pong received: {:?}", message.payload);
                },
                Type::Binary => {
                    return Err(ErrorKind::UnexpectedFormat.into());
                },
            }
        }
        let response = Response {
            items: items,
            last: self.finished,
        };
        Ok(response)
    }

    pub fn end_interaction(mut self) -> Result<Client<S>> {
        if !self.finished {
            let event = Event {
                event: EventKind::Cancel,
                data: None,
            };
            let message = Message::text(serde_json::to_string(&event)?);
            self.inner.send_message(&message)?;
        }
        let client = Client {
            inner: self.inner,
        };
        Ok(client)
    }

}


#[cfg(test)]
mod test {
    use websocket::ClientBuilder;
    use websocket::stream::TcpStream;
    use super::*;

    #[test]
    fn service_test() {
        let client: Client<TcpStream> = ClientBuilder::new("ws://127.0.0.1:10080/")
            .unwrap().connect_insecure().unwrap().into();
        let request = InteractionRequest {
            service: "auth-service".into(),
            action: "do-login".into(),
            payload: Map::new(),
        };
        let mut bclient = client.start_interaction(request).unwrap();
        loop {
            let resp = bclient.send_next(None).unwrap();
            if resp.last {
                break;
            }
        }
    }
}
