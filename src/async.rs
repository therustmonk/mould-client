use std::marker::PhantomData;
use serde_json;
use url::Url;
use serde::de::Deserialize;
use serde_json::Value;
use serde_json::value::ToJson;
use futures::{future, Future, Async, AsyncSink, Poll, Stream, Sink, StartSend, BoxFuture};
use tokio_io::{AsyncRead, AsyncWrite};
use tungstenite::Message;
use tokio_tungstenite::{client_async, ConnectAsync, WebSocketStream};
use super::{Event, EventKind, Error, ErrorKind, InteractionRequest, Request};

pub fn mould_connect<S: AsyncRead + AsyncWrite>(url: Url, stream: S) -> Connecting<S> {
    Connecting {
        inner: client_async(url, stream),
    }
}

pub struct Connecting<S> {
    inner: ConnectAsync<S>,
}

impl<S: AsyncRead + AsyncWrite> Future for Connecting<S> {
    type Item = MouldTransport<S>;
    type Error = Error;

    fn poll(&mut self) -> Poll<MouldTransport<S>, Error> {
        self.inner.poll().map(|async| {
            async.map(MouldTransport::new)
        })
        .map_err(Error::from)
    }
}

pub struct MouldTransport<S> {
    inner: WebSocketStream<S>,
}

impl<S> MouldTransport<S> {
    fn new(wss: WebSocketStream<S>) -> Self {
        MouldTransport {
            inner: wss,
        }
    }
}

impl<T> Stream for MouldTransport<T> where T: AsyncRead + AsyncWrite {
    type Item = Event;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Event>, Error> {
        match self.inner.poll() {
            Ok(Async::Ready(Some(Message::Text(ref text)))) => {
                let event = serde_json::from_str(text)?;
                Ok(Async::Ready(Some(event)))
            },
            Ok(Async::Ready(None)) => {
                Ok(Async::Ready(None))
            },
            Ok(Async::Ready(Some(_))) => {
                Err(ErrorKind::UnexpectedFormat.into())
            },
            Ok(Async::NotReady) => {
                Ok(Async::NotReady)
            },
            Err(e) => {
                Err(e.into())
            },
        }
    }
}

impl<T> Sink for MouldTransport<T> where T: AsyncRead + AsyncWrite {
    type SinkItem = Event;
    type SinkError = Error;

    fn start_send(&mut self, event: Event) -> StartSend<Event, Error> {
        let text = serde_json::to_string(&event)?;
        let message = Message::Text(text);
        self.inner.start_send(message)?; // Put to a send queue
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Error> {
        self.inner.poll_complete().map_err(|e| e.into())
    }
}

impl<T> MouldTransport<T> where T: AsyncRead + AsyncWrite + Send + 'static {
    pub fn start_interaction(self, request: InteractionRequest) -> BoxFuture<Self, Error> {
        let event = future::lazy(move || {
            let event = Event {
                event: EventKind::Request,
                data: Some(request.to_json()?),
            };
            Ok(event)
        });
        event.and_then(move |event| {
            self.send(event)
        })
        .boxed()
    }

    pub fn till_end(self) -> BoxFuture<Self, Error> {
        self.into_future()
            .map_err(|e| e.0)
            .and_then(|(event, mould)| {
                if let Some(Event { event, data }) = event {
                    match event {
                        EventKind::Done => {
                            Ok(mould)
                        },
                        EventKind::Reject => {
                            let reason = data.as_ref().and_then(Value::as_str).unwrap_or("<no reject reason>");
                            Err(ErrorKind::ActionRejected(reason.into()).into())
                        },
                        EventKind::Fail => {
                            let reason = data.as_ref().and_then(Value::as_str).unwrap_or("<no fail reason>");
                            Err(ErrorKind::ActionFailed(reason.into()).into())
                        },
                        kind => {
                            return Err(ErrorKind::UnexpectedKind(format!("{:?}", kind)).into());
                        },
                    }
                } else {
                    Err(ErrorKind::Interrupted.into())
                }
            })
            .boxed()
    }

    pub fn ready_next(self) -> BoxFuture<Self, Error> {
        self.into_future()
            .map_err(|e| e.0)
            .and_then(|(evt, mould)| {
                match evt {
                    Some(Event { event: EventKind::Ready, .. }) => {
                        mould.send(Event::empty(EventKind::Next))
                            .boxed()
                    },
                    Some(event) => {
                        future::err(ErrorKind::UnexpectedKind(format!("{:?}", event.event)).into())
                            .boxed()
                    },
                    None => {
                        future::err(ErrorKind::Interrupted.into())
                            .boxed()
                    },
                }
            })
            .boxed()
    }

    pub fn recv_item(self) -> BoxFuture<(Value, Self), Error> {
        self.into_future()
            .map_err(|e| e.0)
            .and_then(|(evt, mould)| {
                match evt {
                    Some(Event { event: EventKind::Item, data: Some(value) }) => {
                        Ok((value, mould))
                    },
                    Some(event) => {
                        Err(ErrorKind::UnexpectedKind(format!("{:?}", event.event)).into())
                    },
                    None => {
                        Err(ErrorKind::Interrupted.into())
                    },
                }
            })
            .boxed()
    }
}

pub trait MouldStream {
    fn items_flow<T, A>(self, answers: A) -> ItemsFlow<T, Self, A>
        where Self: Sized + Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
              A: Stream<Item=Option<Request>, Error=Error>,
              T: Deserialize

    {
        ItemsFlow::new(self, answers)
    }

    fn send_request<T>(self, initiatior: InteractionRequest) -> TillDone<T, Self>
        where Self: Sized + Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
    {
        TillDone::new(self, initiatior)
    }
}

impl<S> MouldStream for S
    where S: Stream<Item=Event, Error=Error>
{
}

pub struct TillDone<T, S> {
    value: Option<T>,
    stream: Option<S>,
    initiator: Option<InteractionRequest>,
}

impl<T, S> TillDone<T, S>
    where S: Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
{
    pub fn new(s: S, i: InteractionRequest) -> Self {
        TillDone {
            value: None,
            stream: Some(s),
            initiator: Some(i),
        }
    }
}

impl<T, S> Future for TillDone<T, S>
    where S: Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
          T: Deserialize
{
    type Item = (Option<T>, S);
    type Error = (S::Error, S);

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let result = {
            if let Some(inititator) = self.initiator.take() {
                match inititator.to_json() {
                    Ok(request) => {
                        let event = Event {
                            event: EventKind::Request,
                            data: Some(request),
                        };
                        let stream = self.stream.as_mut().expect("polling TillDone twice");
                        stream.start_send(event).map_err(Error::from).map(|_| Async::NotReady)
                    },
                    Err(err) => {
                        Err(err.into())
                    },
                }
            } else {
                let stream = self.stream.as_mut().expect("polling TillDone twice");
                match transform(stream.poll()) {
                    Ok(Async::Ready(Some(value))) => {
                        self.value = Some(value);
                        // We have to wait till `done` event
                        Ok(Async::NotReady)
                    },
                    Ok(Async::Ready(None)) => {
                        Ok(Async::Ready(self.value.take()))
                    },
                    err => {
                        err
                    },
                }
            }
        };
        match result {
            Ok(async) => {
                Ok(async.map(|value| {
                    let stream = self.stream.take().unwrap();
                    (value, stream)
                }))
            },
            Err(err) => {
                let stream = self.stream.take().unwrap();
                Err((err, stream))
            },
        }
    }
}

pub struct ItemsFlow<T, S, A> {
    what: PhantomData<T>,
    stream: S,
    answers: A,
}

impl<T, S, A> ItemsFlow<T, S, A>
    where S: Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
          A: Stream<Item=Option<Request>, Error=Error>,
{
    pub fn new(s: S, a: A) -> Self {
        ItemsFlow {
            what: PhantomData,
            stream: s,
            answers: a,
        }
    }
}

impl<T, S, A> Stream for ItemsFlow<T, S, A>
    where S: Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
          A: Stream<Item=Option<Request>, Error=Error>,
          T: Deserialize,
{
    type Item = T;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<T>, S::Error> {
        let result = transform(self.stream.poll());
        if let Ok(Async::NotReady) = result {
            if let Async::Ready(Some(request)) = self.answers.poll()? {
                let value = match request {
                    Some(request) => Some(request.to_json()?),
                    None => None,
                };
                let event = Event {
                    event: EventKind::Next,
                    data: value,
                };
                self.stream.start_send(event)?;
            }
            Ok(Async::NotReady)
        } else {
            result
        }
    }
}


fn transform<T: Deserialize>(result: Result<Async<Option<Event>>, Error>) -> Result<Async<Option<T>>, Error> {
    match result {
        Ok(Async::Ready(Some(Event { event, data }))) => {
            match event {
                EventKind::Item => {
                    if let Some(data) = data {
                        let res = serde_json::from_value(data);
                        if let Ok(res) = res {
                            Ok(Async::Ready(Some(res)))
                        } else {
                            Err(ErrorKind::UnexpectedFormat.into())
                        }
                    } else {
                        Err(ErrorKind::NoDataProvided.into())
                    }
                },
                EventKind::Reject => {
                    let reason = data.as_ref().and_then(Value::as_str).unwrap_or("<no reject reason>");
                    Err(ErrorKind::ActionRejected(reason.into()).into())
                },
                EventKind::Fail => {
                    let reason = data.as_ref().and_then(Value::as_str).unwrap_or("<no fail reason>");
                    Err(ErrorKind::ActionFailed(reason.into()).into())
                },
                EventKind::Ready => {
                    Ok(Async::NotReady)
                },
                EventKind::Done => {
                    Ok(Async::Ready(None))
                },
                kind => {
                    Err(ErrorKind::UnexpectedKind(format!("{:?}", kind)).into())
                },
            }
        },
        Ok(Async::Ready(None)) => {
            Ok(Async::Ready(None))
        },
        Ok(Async::NotReady) => {
            Ok(Async::NotReady)
        },
        Err(e) => {
            Err(e.into())
        },
    }
}
