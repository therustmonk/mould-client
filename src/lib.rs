#[macro_use] extern crate log;
#[macro_use] extern crate error_chain;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate futures;
extern crate futures_state_stream;
extern crate tokio_core;
extern crate tokio_io;
extern crate tungstenite;
extern crate tokio_tungstenite;
extern crate url;
extern crate mould;

use std::{io, str};
use serde::{Serialize, Deserialize};
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

type Last = bool;

pub struct MouldRequest<R> {
    pub service: String,
    pub action: String,
    pub payload: R,
}

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
            Ok(Async::Ready(Some(Message::Text(ref content)))) => {
                debug!("Recv <= {:?}", content);
                let event = serde_json::from_str(content)?;
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
        let content = serde_json::to_string(&item)?;
        debug!("Send => {:?}", content);
        let message = Message::Text(content);
        self.inner.start_send(message)?; // Put to a send queue
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.inner.poll_complete().map_err(|e| e.into())
    }
}

// Sink I, Stream O
pub struct Interaction<S, R, I, O> {
    request: Option<MouldRequest<R>>,
    item: Option<I>,
    output: PhantomData<O>,
    stream: Option<S>,
    done: bool,
}

pub trait Origin<S> {
    fn origin(self) -> S;
}

impl<S, R, I, O> Origin<S> for Interaction<S, R, I, O> {
    fn origin(mut self) -> S {
        self.stream.take().unwrap()
    }
}

impl<S, R, I, O> Sink for Interaction<S, R, I, O>
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

impl<S, R, I, O> Stream for Interaction<S, R, I, O>
    where S: Stream<Item=Output, Error=Error> + Sink<SinkItem=Input, SinkError=Error>,
          R: Serialize,
          for <'de> I: Deserialize<'de>,
{
    type Item = (I, Last);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        const FAIL: &str = "polling Interaction twice";
        if self.done {
            return Ok(Async::Ready(None));
        }
        if let Some(MouldRequest { service, action, payload }) = self.request.take() {
            let payload = serde_json::to_value(payload)?;
            let sink = self.stream.as_mut().expect(FAIL);
            sink.start_send(Input::Request { service, action, payload })?;
        }
        loop {
            let item = self.stream.as_mut().expect(FAIL).poll();
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
                                let sink = self.stream.as_mut().expect(FAIL);
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

    fn start_interaction<R, I, O>(self, request: MouldRequest<R>)
        -> Interaction<Self, R, I, O>
        where Self: Sized, R: Serialize, for <'de> I: Deserialize<'de>, O: Serialize,
    {
        Interaction {
            request: Some(request),
            item: None,
            output: PhantomData,
            stream: Some(self),
            done: false,
        }
    }

}

impl<S> MouldStream for S
    where S: Sized + Stream<Item=Output, Error=Error> + Sink<SinkItem=Input, SinkError=Error>,
{
}

pub trait MouldFlow {

    fn till_done<B>(self) -> TillDone<Self, B>
        where Self: Stream<Item=((), Last), Error=Error> + Sink<SinkItem=(), SinkError=Error> + Origin<B>,
              Self: Sized,
    {
        TillDone {
            stream: Some(self),
            origin: PhantomData,
        }
    }

    fn last_item<B, I>(self) -> LastItem<Self, B, I>
        where Self: Stream<Item=(I, Last), Error=Error> + Sink<SinkItem=(), SinkError=Error> + Origin<B>,
              Self: Sized,
    {
        LastItem {
            stream: Some(self),
            origin: PhantomData,
            item: None,
        }
    }

    fn all_items<B, I>(self) -> AllItems<Self, B, I>
        where Self: Stream<Item=(I, Last), Error=Error> + Sink<SinkItem=(), SinkError=Error> + Origin<B>,
              Self: Sized,
    {
        AllItems {
            stream: Some(self),
            origin: PhantomData,
            items: Some(Vec::new()),
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

impl<S, R, I, O> MouldFlow for Interaction<S, R, I, O> {
}

pub struct AllItems<S, B, I>
{
    stream: Option<S>,
    origin: PhantomData<B>,
    items: Option<Vec<I>>,
}

impl<S, B, I> Future for AllItems<S, B, I>
    where S: Stream<Item=(I, Last), Error=Error> + Sink<SinkItem=(), SinkError=Error> + Origin<B>,
{
    type Item = (Vec<I>, B);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        const FAIL: &str = "polling AllItems twice";
        loop {
            let item = self.stream.as_mut().expect(FAIL).poll();
            let value = try_ready!(item);
            match value {
                Some((item, false)) => {
                    self.items.as_mut().expect(FAIL).push(item);
                    let sink = self.stream.as_mut().expect(FAIL);
                    sink.start_send(())?;
                },
                Some((item, true)) => {
                    let mut items = self.items.take().expect(FAIL);
                    items.push(item);
                    let stream = self.stream.take().unwrap().origin();
                    return Ok(Async::Ready((items, stream)));
                },
                None => {
                    let items = self.items.take().expect(FAIL);
                    let stream = self.stream.take().unwrap().origin();
                    return Ok(Async::Ready((items, stream)));
                },
            }
        }
    }
}

pub struct LastItem<S, B, I>
{
    stream: Option<S>,
    origin: PhantomData<B>,
    item: Option<I>,
}

impl<S, B, I> Future for LastItem<S, B, I>
    where S: Stream<Item=(I, Last), Error=Error> + Sink<SinkItem=(), SinkError=Error> + Origin<B>,
{
    type Item = (Option<I>, B);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        const FAIL: &str = "polling LastItem twice";
        loop {
            let item = self.stream.as_mut().expect(FAIL).poll();
            let value = try_ready!(item);
            match value {
                Some((item, false)) => {
                    self.item = Some(item);
                    let sink = self.stream.as_mut().expect(FAIL);
                    sink.start_send(())?;
                },
                Some((item, true)) => {
                    let stream = self.stream.take().unwrap().origin();
                    return Ok(Async::Ready((Some(item), stream)));
                },
                None => {
                    let item = self.item.take();
                    let stream = self.stream.take().unwrap().origin();
                    return Ok(Async::Ready((item, stream)));
                },
            }
        }
    }
}

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
        const FAIL: &str = "polling TillDone twice";
        loop {
            let item = self.stream.as_mut().expect(FAIL).poll();
            let value = try_ready!(item);
            if let Some((_, false)) = value {
                let sink = self.stream.as_mut().expect(FAIL);
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

impl<S, B, T, F, R, I, O> Future for FoldFlow<S, B, T, F, R>
    where S: Stream<Item=(I, Last), Error=Error> + Sink<SinkItem=O, SinkError=Error> + Origin<B>,
          F: FnMut((T, I)) -> R, Self: Sized,
          R: IntoFuture<Item=(T, O), Error=S::Error>,
{
    type Item = (T, B);
    type Error = Error; // TODO Repair the stream

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        const FAIL: &str = "polling FoldFlow twice";
        loop {
            if self.pending.is_some() {
                match self.pending.as_mut().unwrap().poll() {
                    Ok(Async::Ready((fold, res))) => {
                        self.fold = Some(fold);
                        self.pending = None;
                        if !self.done {
                            let sink = self.stream.as_mut().expect(FAIL);
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
                let item = self.stream.as_mut().expect(FAIL).poll();
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

