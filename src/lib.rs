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
extern crate mould;

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
use mould::session::{Input, Output};


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
    type Item = Output;
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
    type SinkItem = Input;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let text = serde_json::to_string(&item)?;
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
    where S: Sink<SinkItem=Input, SinkError=Error>,
          O: Serialize,
{
    type SinkItem = O;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if let Some(ref mut stream) = self.stream {
            let data = serde_json::to_value(&item)?;
            stream.start_send(Input::Next(data))?; // Put to a send queue
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
    where S: Stream<Item=Output, Error=Error> + Sink<SinkItem=Input, SinkError=Error>,
          R: Serialize,
          for <'de> I: Deserialize<'de>,
{
    type Item = (I, Last);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.done {
            return Ok(Async::Ready(None));
        }
        if let Some(InteractionRequest { service, action, payload }) = self.request.take() {
            let payload = serde_json::to_value(payload)?;
            let sink = self.stream.as_mut().expect("polling StartInteraction twice");
            sink.start_send(Input::Request { service, action, payload })?;
        }
        loop {
            let item = self.stream.as_mut().expect("polling FoldFlow twice").poll();
            match try_ready!(item) {
                Some(output) => {
                    match output {
                        Output::Item(data) => {
                            let item = serde_json::from_value(data)?;
                            self.item = Some(item);
                        },
                        Output::Ready => {
                            if let Some(item) = self.item.take() {
                                return Ok(Async::Ready(Some((item, false))));
                            } else {
                                let sink = self.stream.as_mut().expect("polling FoldFlow twice");
                                sink.start_send(Input::Next(Value::Null))?;
                            }
                        },
                        Output::Reject(reason) => {
                            self.done = true;
                            return Err(ErrorKind::ActionRejected(reason).into());
                        },
                        Output::Fail(reason) => {
                            self.done = true;
                            return Err(ErrorKind::ActionFailed(reason).into());
                        },
                        Output::Done => {
                            self.done = true;
                            if let Some(item) = self.item.take() {
                                return Ok(Async::Ready(Some((item, true))));
                            } else {
                                return Ok(Async::Ready(None));
                            }
                        },
                        Output::Suspended(_) => {
                            return Err("task suspending not supported".into());
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
    where S: Sized + Stream<Item=Output, Error=Error> + Sink<SinkItem=Input, SinkError=Error>,
{
}

pub trait MouldFlow {

    // TODO one_item
    // TODO all_items

    fn till_done<B>(self) -> TillDone<Self, B>
        where Self: Stream<Item=((), Last), Error=Error> + Sink<SinkItem=(), SinkError=Error> + Origin<B>,
              Self: Sized,
    {
        TillDone {
            stream: Some(self),
            origin: PhantomData,
        }
    }

    fn fold_flow<S, B, T, F, R, I, O>(self, init: T, f: F) -> FoldFlow<Self, B, T, F, R>
        where R: IntoFuture<Item=(S, O)>,
              F: FnMut((T, I)) -> R,
              Self: Sized,
    {
        FoldFlow {
            stream: Some(self),
            origin: PhantomData,
            fold: Some(init),
            pending: None,
            f: f,
            done: false,
        }
    }
}

impl<S, R, I, O> MouldFlow for BeginInteraction<S, R, I, O> {
}

/*
impl<S, I, O> MouldFlow for S
    where S: Sized + Stream<Item=(I, Last), Error=Error> + Sink<SinkItem=O, SinkError=Error>,
{
}
*/

pub struct TillDone<S, B>
{
    stream: Option<S>,
    origin: PhantomData<B>,
}

impl<S, B> Future for TillDone<S, B>
    where S: Stream<Item=((), Last), Error=Error> + Sink<SinkItem=(), SinkError=Error> + Origin<B>,
{
    type Item = B;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let item = self.stream.as_mut().expect("polling FoldFlow twice").poll();
            let value = try_ready!(item);
            if let Some((_, false)) = value {
                let sink = self.stream.as_mut().expect("polling FoldFlow twice");
                sink.start_send(())?;
            } else {
                let stream = self.stream.take().unwrap().origin();
                return Ok(Async::Ready(stream));
            }
        }
    }
}

pub struct FoldFlow<S, B, T, F, R>
    where R: IntoFuture,
{
    stream: Option<S>,
    origin: PhantomData<B>,
    fold: Option<T>,
    pending: Option<R::Future>,
    f: F,
    done: bool,
}

type Last = bool;

/// Option<I> - None if last
/// Option<O> - None if last
impl<S, B, T, F, R, I, O> Future for FoldFlow<S, B, T, F, R>
    where S: Stream<Item=(I, Last), Error=Error> + Sink<SinkItem=O, SinkError=Error> + Origin<B>,
          F: FnMut((T, I)) -> R, Self: Sized,
          R: IntoFuture<Item=(T, O), Error=S::Error>,
{
    type Item = (T, B);
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
                            // TODO Consider to fire an error
                            // because this interaction process must expect response for every
                            // question!!!!!!!!!!!!!
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
