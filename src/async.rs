use serde_json;
use url::Url;
use serde_json::Value;
use serde_json::value::ToJson;
use futures::{future, Future, Async, AsyncSink, Poll, Stream, Sink, StartSend, BoxFuture};
use tokio_io::{AsyncRead, AsyncWrite};
use tungstenite::Message;
use tokio_tungstenite::{client_async, ConnectAsync, WebSocketStream};
use super::{Event, EventKind, Error, ErrorKind, InteractionRequest};
//use futures_state_stream::{unfold, StateStream, StreamExt};

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
        }).map_err(Error::from)
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

pub struct MouldClient<T>(MouldTransport<T>);

impl<T> MouldClient<T> where T: AsyncRead + AsyncWrite + Send + 'static {
    pub fn start_interaction(self, request: InteractionRequest) -> BoxFuture<Self, Error> {
        let event = future::lazy(move || {
            let event = Event {
                event: EventKind::Request,
                data: Some(request.to_json()?),
            };
            Ok(event)
        });
        event.and_then(move |event| {
            self.0.send(event)
        })
        .map(MouldClient)
        .boxed()
    }

    pub fn recv_item(self) -> BoxFuture<(Value, Self), Error> {
        self.0.into_future()
            .map_err(|e| e.0)
            .and_then(|(evt, mould)| {
                match evt {
                    Some(Event { event: EventKind::Item, data: Some(value) }) => {
                        Ok((value, MouldClient(mould)))
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

    /*
    pub fn till_end(self) -> BoxFuture<Self, Error> {
        unfold(self.0, |c| {
            let (sink, stream) = c.split();
            stream.take_while(|event| Ok(event.is_terminated()))
                .filter(Event::is_ready)
                .map(|_| {
                    Event {
                        event: EventKind::Next,
                        data: None,
                    }
                })
                .forward(sink)
        }).boxed()
    }
    */

    /*
    pub fn till_end(self) -> BoxFuture<Self, Error> {
        let (sink, stream) = self.split();
        self.take_while(|event| Ok(event.is_terminated()))
            .into_future()
            .map_err(|e| e.0)
            .and_then(|t| Ok(t.1))
            .boxed()
            .into_future()
            /*
            .filter(Event::is_ready)
            .map(|_| {
                Event {
                    event: EventKind::Next,
                    data: None,
                }
            })
            .forward(sink)
            .map(|t| t.0)
            .boxed()
            */
        /*
            .map(|event| event.data)
            .filter(|data| data.is_some())
            .map(Option::unwrap)
            .collect()
            .into_stream()
            .into_future()
            .map_err(|e| e.0)
            .and_then(|t| t.1)
            .boxed();
        fut
        */
            /*
            .and_then(|(vec, mould)| {
                if let Some(vec) = vec {
                    return mould.and_then(|mould| {
                        Ok((vec, mould))
                    }).boxed();
                } else {
                    return future::err(Err(ErrorKind::Interrupted.into())).boxed();
                }
            }).boxed()
            */
            /*
            .and_then(|(vec, mould)| {
                mould.and_then(|mould| {
                    if let Some(vec) = vec {
                        Ok((vec, mould))
                    } else {
                        Err(ErrorKind::Interrupted.into())
                    }
                })
            })
            */
    }
    */
}

/*
impl<T> MouldTransport<T> where T: AsyncRead + AsyncWrite + Send + 'static {
    pub fn one_shot(self, request: InteractionRequest) -> BoxFuture<Self, Error> {
        let event = future::lazy(move || {
            let event = Event {
                event: EventKind::Request,
                data: Some(request.to_json()?),
            };
            Ok(event)
        });
        let send_req = event.and_then(|event| {
            self.send(event)
        });
        let wait_done = send_req.and_then(|mould| {
            mould.take_while(|e| future::ok(e.is_terminated())).collect().fuse()
        });
        wait_done.boxed()
    }
}
*/
