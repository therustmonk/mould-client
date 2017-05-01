#[macro_use] extern crate log;
#[macro_use] extern crate error_chain;
extern crate serde;
#[macro_use] extern crate serde_derive;
pub extern crate serde_json;
#[macro_use] extern crate futures;
extern crate futures_state_stream;
extern crate tokio_core;
extern crate tokio_io;
extern crate tungstenite;
extern crate tokio_tungstenite;
extern crate url;

use std::{fmt, io, str, result};
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::de::{Visitor};
use serde_json::Value;
use std::marker::PhantomData;
use url::Url;
use futures::{Future, IntoFuture, Async, AsyncSink, Poll, Stream, Sink, StartSend};
use tokio_io::{AsyncRead, AsyncWrite};
use tungstenite::Message;
use tokio_tungstenite::{client_async, ConnectAsync, WebSocketStream};


error_chain! {
    foreign_links {
        IoError(io::Error);
        EncodingError(str::Utf8Error);
        SerdeError(serde_json::Error);
        AsycWebSocketError(tungstenite::Error);
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
        Other(s: String) {
            description("other error")
            display("other error: '{}'", s)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    pub event: EventKind,
    pub data: Option<Value>,
}

impl Event {
    pub fn is_terminated(&self) -> bool {
        use EventKind::*;
        match self.event {
            Done | Fail | Reject => true,
            _ => false,
        }
    }

    pub fn is_ready(&self) -> bool {
        use EventKind::*;
        match self.event {
            Ready => true,
            _ => false,
        }
    }

    pub fn empty(kind: EventKind) -> Self {
        Event {
            event: kind,
            data: None,
        }
    }
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

impl<'de> Deserialize<'de> for EventKind {
    fn deserialize<D>(deserializer: D) -> result::Result<EventKind, D::Error>
        where D: Deserializer<'de>
    {
        struct FieldVisitor {
            min: usize,
        };

        impl<'vi> Visitor<'vi> for FieldVisitor {
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
                    "item" => EventKind::Item,
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


#[derive(Serialize)]
pub struct InteractionRequest<R> {
    pub service: String,
    pub action: String,
    pub payload: R,
}

pub type Request = Value;

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

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
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

    fn start_send(&mut self, event: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let text = serde_json::to_string(&event)?;
        let message = Message::Text(text);
        self.inner.start_send(message)?; // Put to a send queue
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.inner.poll_complete().map_err(|e| e.into())
    }
}

// Sink I, Stream O
pub struct BeginInteraction<S, R, I, O> {
    request: Option<InteractionRequest<R>>,
    item: Option<I>,
    output: PhantomData<O>,
    stream: Option<S>,
    done: bool,
}

pub trait Origin<S> {
    fn origin(self) -> S;
}

impl<S, R, I, O> Origin<S> for BeginInteraction<S, R, I, O> {
    fn origin(mut self) -> S {
        self.stream.take().unwrap()
    }
}

impl<S, R, I, O> Sink for BeginInteraction<S, R, I, O>
    where S: Sink<SinkItem=Event, SinkError=Error>,
          O: Serialize,
{
    type SinkItem = Option<O>;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if let Some(ref mut stream) = self.stream {
            let data = {
                if let Some(item) = item {
                    Some(serde_json::to_value(&item)?)
                } else {
                    None
                }
            };
            let event = EventKind::Next;
            stream.start_send(Event { event, data })?; // Put to a send queue
            Ok(AsyncSink::Ready)
        } else {
            Ok(AsyncSink::NotReady(item))
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        if let Some(ref mut stream) = self.stream {
            stream.poll_complete().map_err(|e| e.into())
        } else {
            Ok(Async::Ready(()))
        }
    }
}

impl<S, R, I, O> Stream for BeginInteraction<S, R, I, O>
    where S: Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
          R: Serialize,
          for <'de> I: Deserialize<'de>,
{
    type Item = (I, Last);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.done {
            return Ok(Async::Ready(None));
        }
        if let Some(request) = self.request.take() {
            let request = serde_json::to_value(request)?;
            let stream = self.stream.as_mut().expect("polling StartInteraction twice");
            let event = Event {
                event: EventKind::Request,
                data: Some(request),
            };
            stream.start_send(event)?;
        }
        loop {
            let item = self.stream.as_mut().expect("polling FoldFlow twice").poll();
            match try_ready!(item) {
                Some(Event { event, data }) => {
                    match event {
                        EventKind::Item => {
                            if let Some(data) = data {
                                let item = serde_json::from_value(data)?;
                                self.item = Some(item);
                            } else {
                                return Err("take item twice".into());
                            }
                        },
                        EventKind::Ready => {
                            if let Some(item) = self.item.take() {
                                return Ok(Async::Ready(Some((item, false))));
                            } else {
                                let event = Event {
                                    event: EventKind::Next,
                                    data: None,
                                };
                                let sink = self.stream.as_mut().expect("polling FoldFlow twice");
                                sink.start_send(event)?;
                                //return Err("no item taken".into());
                            }
                        },
                        EventKind::Reject => {
                            self.done = true;
                            let reason = data.as_ref().and_then(Value::as_str).unwrap_or("<no reject reason>");
                            return Err(ErrorKind::ActionRejected(reason.into()).into());
                        },
                        EventKind::Fail => {
                            self.done = true;
                            let reason = data.as_ref().and_then(Value::as_str).unwrap_or("<no fail reason>");
                            return Err(ErrorKind::ActionFailed(reason.into()).into());
                        },
                        EventKind::Done => {
                            self.done = true;
                            if let Some(item) = self.item.take() {
                                return Ok(Async::Ready(Some((item, true))));
                            } else {
                                return Ok(Async::Ready(None));
                            }
                        },
                        kind => {
                            // TODO Send `cancel` event
                            return Err(ErrorKind::UnexpectedKind(format!("{:?}", kind)).into());
                        },
                    }
                },
                None => {
                    return Ok(Async::Ready(None));
                },
            }
        }
    }
}

pub trait MouldStream {

    fn start_interaction<R, I, O>(self, request: InteractionRequest<R>)
        -> BeginInteraction<Self, R, I, O>
        where Self: Sized, R: Serialize, for <'de> I: Deserialize<'de>, O: Serialize,
    {
        BeginInteraction {
            request: Some(request),
            item: None,
            output: PhantomData,
            stream: Some(self),
            done: false,
        }
    }

    /*
    fn do_interaction<T, F, I, R, O, D>(self, service: String, action: String, data: D, init: T, f: F) -> Result<FoldFlow<T, F, I, R, O, Self>>
        where R: IntoFuture<Item=(T, Option<O>)>, Self: Sized,
              F: FnMut((T, I)) -> R, Self: Sized,
              D: Serialize
    {
        // TODO Making interaction request
        let payload = serde_json::to_value(data)?;
        let request = InteractionRequest { service, action, payload };
        Ok(FoldFlow::new(self, init, request, f))
    }
    */

}



impl<S> MouldStream for S
    where S: Sized + Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
{
}

pub trait MouldFlow {
    fn fold_flow<S, X, T, F, R, I, O>(self, init: T, f: F) -> FoldFlow<Self, X, T, F, R>
        where R: IntoFuture<Item=(S, O)>,
              F: FnMut((T, I)) -> R,
              Self: Sized,
    {
        FoldFlow {
            fold: Some(init),
            stream: Some(self),
            pending: None,
            f: f,
            origin: PhantomData,
            done: false,
        }
    }
}

impl<S, I, O> MouldFlow for S
    where S: Sized + Stream<Item=I, Error=Error> + Sink<SinkItem=O, SinkError=Error>,
{
}

pub struct FoldFlow<S, X, T, F, R>
    where R: IntoFuture,
{
    fold: Option<T>,
    stream: Option<S>,
    pending: Option<R::Future>,
    f: F,
    origin: PhantomData<X>,
    done: bool,
}

type Last = bool;

/// Option<I> - None if last
/// Option<O> - None if last
impl<S, X, T, F, R, I, O> Future for FoldFlow<S, X, T, F, R>
    where S: Stream<Item=(I, Last), Error=Error> + Sink<SinkItem=O, SinkError=Error> + Origin<X>,
          F: FnMut((T, I)) -> R, Self: Sized,
          R: IntoFuture<Item=(T, O), Error=S::Error>,
{
    type Item = (T, X);
    type Error = Error; // TODO Repair the stream

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            if self.pending.is_some() {
                match self.pending.as_mut().unwrap().poll() {
                    Ok(Async::Ready((fold, res))) => {
                        self.fold = Some(fold);
                        self.pending = None;
                        if !self.done {
                            let sink = self.stream.as_mut().expect("polling FoldFlow twice");
                            sink.start_send(res)?;
                        } else {
                            break;
                        }
                    },
                    Err(err) => {
                        self.pending = None;
                        return Err(err);
                    },
                    Ok(Async::NotReady) => {
                        return Ok(Async::NotReady);
                    },
                }
            } else {
                let item = self.stream.as_mut().expect("polling FoldFlow twice").poll();
                let value = try_ready!(item);
                if let Some((value, last)) = value {
                    self.done = last;
                    let fold = self.fold.take().unwrap();
                    let fut = (self.f)((fold, value)).into_future();
                    self.pending = Some(fut);
                } else {
                    break;
                }
            }
        }
        let fold = self.fold.take().unwrap();
        let stream = self.stream.take().unwrap().origin();
        Ok(Async::Ready((fold, stream)))
    }
}

/*
pub struct FoldFlow<T, F, I, R, O, S>
    where R: IntoFuture
{
    fold: Option<T>,
    request: Option<InteractionRequest>,
    need_next: bool,
    is_done: bool,
    stream: Option<S>,
    f: F,
    pending: Option<R::Future>,
    input: PhantomData<I>,
    output: PhantomData<O>,
}

impl<T, F, I, R, O, S> FoldFlow<T, F, I, R, O, S>
    where R: IntoFuture
{
    fn new(s: S, init: T, i: InteractionRequest, f: F) -> Self {
        FoldFlow {
            fold: Some(init),
            request: Some(i),
            need_next: true,
            is_done: false,
            stream: Some(s),
            f: f,
            pending: None,
            input: PhantomData,
            output: PhantomData,
        }
    }
}

impl<T, F, I, R, O, S> Future for FoldFlow<T, F, I, R, O, S>
    where S: Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
          F: FnMut((T, I)) -> R, Self: Sized,
          R: IntoFuture<Item=(T, Option<O>), Error=S::Error>,
          for<'de> I: Deserialize<'de>,
          O: Serialize,
{
    type Item = (T, S);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(request) = self.request.take() {
            match serde_json::to_value(request) {
                Ok(request) => {
                    let stream = self.stream.as_mut().expect("polling StartInteraction twice");
                    let event = Event {
                        event: EventKind::Request,
                        data: Some(request),
                    };
                    stream.start_send(event)?;
                },
                Err(err) => {
                    return Err(err.into());
                },
            }
        }
        let res = self.pending.as_mut().map(|fut| fut.poll());
        match res {
            Some(Ok(Async::Ready((fold, value)))) => {
                if self.is_done {
                    let stream = self.stream.take().unwrap();
                    let fold = self.fold.take().unwrap();
                    return Ok(Async::Ready((fold, stream)));
                } else {
                    let value = serde_json::to_value(value)?;
                    let event = Event {
                        event: EventKind::Next,
                        data: Some(value),
                    };
                    let sink = self.stream.as_mut().expect("polling FoldFlow twice");
                    sink.start_send(event)?;
                    self.fold = Some(fold);
                    self.pending = None;
                    // No need to send `cancel`, because impossible
                }
            },
            None | Some(Ok(Async::NotReady)) => {
            },
            Some(Err(_)) => {
                // TODO Send cancel
                return Err(ErrorKind::Interrupted.into());
            },
        }
        loop {
            let item = self.stream.as_mut().expect("polling FoldFlow twice").poll();
            match try_ready!(item) {
                Some(Event { event, data }) => {
                    trace!("Event {:?} received with data {:?}", event, data);
                    match event {
                        EventKind::Item => {
                            if let Some(data) = data {
                                let res = serde_json::from_value(data);
                                if let Ok(value) = res {
                                    if let Some(fold) = self.fold.take() {
                                        let fut = (self.f)((fold, value)).into_future();
                                        self.pending = Some(fut);
                                    }
                                } else {
                                    // TODO Send `cancel` event
                                    return Err(ErrorKind::UnexpectedFormat.into());
                                }
                            } else {
                                // TODO Send `cancel` event
                                return Err(ErrorKind::NoDataProvided.into());
                            }
                        },
                        EventKind::Ready => {
                            if self.need_next {
                                let stream = self.stream.as_mut().expect("polling StartInteraction twice");
                                let event = Event {
                                    event: EventKind::Next,
                                    data: None,
                                };
                                stream.start_send(event)?;
                                self.need_next = false;
                            } else {
                                let res = self.pending.as_mut().map(|fut| fut.poll());
                                match res {
                                    Some(Ok(Async::Ready((fold, value)))) => {
                                        let value = serde_json::to_value(value)?;
                                        let event = Event {
                                            event: EventKind::Next,
                                            data: Some(value),
                                        };
                                        let sink = self.stream.as_mut().expect("polling FoldFlow twice");
                                        sink.start_send(event)?;
                                        self.fold = Some(fold);
                                        self.pending = None;
                                        // No need to send `cancel`, because impossible
                                    },
                                    None | Some(Ok(Async::NotReady)) => {
                                    },
                                    Some(Err(_)) => {
                                        // TODO Send cancel
                                        return Err(ErrorKind::Interrupted.into());
                                    },
                                }
                            }
                        },
                        EventKind::Reject => {
                            let reason = data.as_ref().and_then(Value::as_str).unwrap_or("<no reject reason>");
                            return Err(ErrorKind::ActionRejected(reason.into()).into());
                        },
                        EventKind::Fail => {
                            let reason = data.as_ref().and_then(Value::as_str).unwrap_or("<no fail reason>");
                            return Err(ErrorKind::ActionFailed(reason.into()).into());
                        },
                        EventKind::Done => {
                            self.is_done = true;
                            let res = self.pending.as_mut().map(|fut| {
                                fut.poll()
                            });
                            match res {
                                Some(Ok(Async::Ready((fold, _)))) => {
                                    let stream = self.stream.take().unwrap();
                                    return Ok(Async::Ready((fold, stream)));
                                },
                                Some(Ok(Async::NotReady)) => {
                                    // Ignore...
                                },
                                Some(Err(err)) => {
                                    return Err(err);
                                },
                                None => {
                                    let stream = self.stream.take().unwrap();
                                    let fold = self.fold.take().unwrap();
                                    return Ok(Async::Ready((fold, stream)));
                                },
                            }
                        },
                        kind => {
                            // TODO Send `cancel` event
                            return Err(ErrorKind::UnexpectedKind(format!("{:?}", kind)).into());
                        },
                    }
                },
                None => {
                    let stream = self.stream.take().unwrap();
                    let fold = self.fold.take().unwrap();
                    return Ok(Async::Ready((fold, stream)));
                },
            }
        }
    }
}
*/
