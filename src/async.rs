use std::marker::PhantomData;
use serde_json;
use url::Url;
use serde::de::Deserialize;
use serde_json::Value;
use serde_json::value::ToJson;
use futures::{Future, IntoFuture, Async, AsyncSink, Poll, Stream, Sink, StartSend};
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

pub trait MouldStream {
    fn items_flow<T, A>(self, answers: A) -> ItemsFlow<T, Self, A>
        where Self: Sized + Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
              A: Stream<Item=Option<Request>, Error=Error>,
              T: Deserialize

    {
        ItemsFlow::new(self, answers)
    }

    fn start_interaction(self, initiatior: InteractionRequest) -> StartInteraction<Self>
        where Self: Sized + Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
    {
        StartInteraction::new(self, initiatior)
    }

    fn till_done<T>(self) -> TillDone<T, Self>
        where Self: Sized + Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
    {
        TillDone::new(self)
    }

}

impl<S> MouldStream for S
    where S: Sized + Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
{
}

pub struct StartInteraction<S> {
    request: Option<InteractionRequest>,
    stream: Option<S>,
}

impl<S> StartInteraction<S> {
    pub fn new(s: S, i: InteractionRequest) -> Self {
        StartInteraction {
            request: Some(i),
            stream: Some(s),
        }
    }
}

impl<S> Future for StartInteraction<S>
    where S: Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
{
    type Item = S;
    type Error = (S::SinkError, S);

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(request) = self.request.take() {
            match request.to_json() {
                Ok(request) => {
                    let event = Event {
                        event: EventKind::Request,
                        data: Some(request),
                    };
                    let sending = self.stream.as_mut().expect("polling StartInteraction twice").start_send(event);
                    if let Err(err) = sending {
                        let stream = self.stream.take().unwrap();
                        return Err((err, stream));
                    }
                },
                Err(err) => {
                    let stream = self.stream.take().unwrap();
                    return Err((err.into(), stream));
                },
            }
        }
        let evt = self.stream.as_mut().expect("polling StartInteraction twice").poll();
        match transform::<Value>(evt) {
            Ok(Async::Ready(_)) => {
                let stream = self.stream.take().unwrap();
                Ok(Async::Ready(stream))
            },
            Ok(Async::NotReady) => {
                Ok(Async::NotReady)
            },
            Err(err) => {
                let stream = self.stream.take().unwrap();
                Err((err, stream))
            },
        }
    }
}

pub struct TillDone<T, S> {
    value: Option<T>,
    stream: Option<S>,
}

impl<T, S> TillDone<T, S> {
    pub fn new(s: S) -> Self {
        TillDone {
            value: None,
            stream: Some(s),
        }
    }
}

impl<T, S> Future for TillDone<T, S>
    where S: Stream<Item=Event, Error=Error> + Sink<SinkItem=Event, SinkError=Error>,
          T: Deserialize
{
    // TODO User Item = (Result<T, MouldReason>, S);
    type Item = (Option<T>, S);
    type Error = (S::Error, S);

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let event = self.stream.as_mut().expect("polling TillDone twice").poll();
        match transform(event) {
            Ok(Async::Ready(Some(Ready::Item(value)))) => {
                self.value = Some(value);
                // We have to wait till `done` event
                Ok(Async::NotReady)
            },
            Ok(Async::Ready(Some(Ready::NeedNext))) => {
                let event = Event {
                    event: EventKind::Next,
                    data: None,
                };
                let res = self.stream.as_mut().expect("polling TillDone twice").start_send(event);
                match res {
                    Ok(_) => {
                        Ok(Async::NotReady)
                    },
                    Err(err) => {
                        // TODO Consider to send `cancel` event
                        let stream = self.stream.take().unwrap();
                        Err((err, stream))
                    },
                }
            },
            Ok(Async::Ready(None)) => {
                let stream = self.stream.take().unwrap();
                let value = self.value.take();
                Ok(Async::Ready((value, stream)))
            },
            Ok(Async::NotReady) => {
                Ok(Async::NotReady)
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
        match result {
            Ok(Async::NotReady) | Ok(Async::Ready(Some(Ready::NeedNext))) => {
                if let Async::Ready(Some(request)) = self.answers.poll()? {
                    let value = request.to_json()?;
                    let event = Event {
                        event: EventKind::Next,
                        data: Some(value),
                    };
                    self.stream.start_send(event)?;
                }
                Ok(Async::NotReady)
            },
            Ok(Async::Ready(Some(Ready::Item(item)))) => {
                Ok(Async::Ready(Some(item)))
            },
            Ok(Async::Ready(None)) => {
                Ok(Async::Ready(None))
            },
            Err(err) => {
                Err(err)
            },
        }
    }
}

enum Ready<T> {
    Item(T),
    NeedNext,
}

fn transform<T: Deserialize>(result: Result<Async<Option<Event>>, Error>)
    -> Result<Async<Option<Ready<T>>>, Error> {
    match result {
        Ok(Async::Ready(Some(Event { event, data }))) => {
            match event {
                EventKind::Item => {
                    if let Some(data) = data {
                        let res = serde_json::from_value(data);
                        if let Ok(res) = res {
                            Ok(Async::Ready(Some(Ready::Item(res))))
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
                    Ok(Async::Ready(Some(Ready::NeedNext)))
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

pub trait Companion {
    fn stright(self) -> Stright<Self>
        where Self: Sized + IntoFuture
    {
        Stright::new(self)
    }
}

impl<F> Companion for F
    where F: Future
{
}

pub struct Stright<S> {
    inner: S,
}

impl<S> Stright<S> {
    pub fn new(s: S) -> Self {
        Stright {
            inner: s,
        }
    }
}

impl<S, L, R> Future for Stright<S>
    where S: Future<Error=(L, R)>
{
    type Item = S::Item;
    type Error = L;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let evt = self.inner.poll();
        evt.map_err(|(l, _)| l)
    }
}

