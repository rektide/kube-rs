//! Generic list-then-watch driver for non-Kubernetes sources.
//!
//! The Kubernetes [`watcher`](crate::watcher()) is a finite state machine over exactly two
//! operations — an initial (possibly paginated) list, and a resumable watch stream — plus a
//! small set of protocol signals: a resume cursor, a page token, cursor-advance events,
//! desync ("your cursor is too old, re-list"), and a liveness timeout. Everything else in it
//! is the Kubernetes *encoding* of those signals (resourceVersion strings, continue tokens,
//! bookmarks, HTTP 410).
//!
//! [`WatchSource`] captures the signals without the encoding, and [`source_watcher`] runs the
//! same FSM shape over any implementation, emitting the standard
//! [`watcher::Event`](crate::watcher::Event) vocabulary. The output stream plugs directly into
//! the rest of this crate: [`reflector`](crate::reflector()) stores,
//! [`WatchStreamExt`](crate::WatchStreamExt) combinators (including backoff), and
//! [`Controller::for_stream`](crate::Controller::for_stream) /
//! [`applier`](crate::applier()).
//!
//! Errors from the source surface as [`watcher::Error::Source`](crate::watcher::Error), so
//! existing error plumbing (e.g. `default_backoff`) applies unchanged.
//!
//! # Example shape
//!
//! ```ignore
//! struct MyApi { /* http / websocket / xrpc client, likely tokio-based */ }
//!
//! impl WatchSource for MyApi {
//!     type Value = MyRecord;          // ideally impl kube_core::Resource for Store/Controller use
//!     type Cursor = String;           // resume token: seq number, GTID, mtime+hash, ...
//!     type PageToken = String;
//!     type Error = MyApiError;
//!     type WatchStream = BoxStream<'static, Result<WatchStep<MyRecord, String>, MyApiError>>;
//!
//!     async fn list(&self, page: Option<String>) -> Result<ListPage<MyRecord, String, String>, MyApiError> {
//!         // GET /records?page=... — return items + final cursor + optional next page token
//!     }
//!     async fn watch(&self, from: String) -> Result<Self::WatchStream, MyApiError> {
//!         // subscribe from cursor — map wire events into WatchStep::{Apply, Delete, Cursor}
//!     }
//!     fn classify(&self, err: &MyApiError) -> ErrorClass {
//!         if err.is_cursor_too_old() { ErrorClass::Desync } else { ErrorClass::Transient }
//!     }
//! }
//!
//! let stream = source_watcher(MyApi::new());
//! // reflector(writer, stream) / Controller::for_stream(stream.applied_objects(), reader) / ...
//! ```

use crate::watcher::{Error, Event};
use futures::{Stream, StreamExt};
use std::{collections::VecDeque, future::Future, time::Duration};
use tracing::{debug, warn};

/// How a [`WatchSource`] error should steer the driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorClass {
    /// The resume cursor is no longer valid; the driver must discard it and re-list.
    ///
    /// The Kubernetes analogue is HTTP 410 GONE from an expired watch window.
    Desync,
    /// Access was denied. Behaves like [`ErrorClass::Transient`] for state purposes, but is
    /// logged loudly since retrying rarely fixes authorization.
    Denied,
    /// Anything else: network failures, timeouts, decode errors. The error is surfaced and
    /// the driver retries from its current state on the next poll (apply backoff downstream,
    /// e.g. [`WatchStreamExt::default_backoff`](crate::WatchStreamExt::default_backoff)).
    Transient,
}

/// One page of the initial list.
pub struct ListPage<K, Cursor, Page> {
    /// The objects on this page.
    pub items: Vec<K>,
    /// The resume cursor as-of this page, if the source knows it.
    ///
    /// The driver remembers the most recent `Some` value; the final page must have produced
    /// one (directly or on an earlier page) or the driver reports
    /// [`Error::NoResourceVersion`] and starts over.
    pub cursor: Option<Cursor>,
    /// Token for the next page, or `None` when this is the final page.
    pub next_page: Option<Page>,
}

/// One event on a [`WatchSource`]'s watch stream.
#[derive(Clone, Debug)]
pub enum WatchStep<K, Cursor> {
    /// An object was created or updated.
    Apply(K),
    /// An object was removed.
    Delete(K),
    /// The resume cursor advanced without object changes (keepalive / bookmark).
    ///
    /// Sources whose data events carry cursors should emit this after the corresponding
    /// [`WatchStep::Apply`]/[`WatchStep::Delete`] so reconnects resume precisely.
    Cursor(Cursor),
}

/// A list-then-watch capable source of objects, in transport-neutral vocabulary.
///
/// Implement this over whatever client the source actually needs — HTTP polling, a WebSocket
/// subscription, a database changefeed, filesystem notification. The driver never sees the
/// transport; it sees pages, cursors, and steps.
pub trait WatchSource {
    /// The object type emitted into [`Event`]s.
    ///
    /// Implement [`Resource`](kube_core::Resource) (or at least
    /// [`Lookup`](crate::reflector::Lookup)) for it if the stream should feed a reflector
    /// [`Store`](crate::reflector::store::Store) or a [`Controller`](crate::Controller).
    type Value: Clone + Send + 'static;
    /// Resume position for [`WatchSource::watch`]: a resourceVersion equivalent.
    type Cursor: Clone + Send;
    /// Pagination token for [`WatchSource::list`].
    type PageToken: Send;
    /// The source's error type; classified by [`WatchSource::classify`].
    type Error: std::error::Error + Send + Sync + 'static;
    /// The stream returned by [`WatchSource::watch`].
    type WatchStream: Stream<Item = Result<WatchStep<Self::Value, Self::Cursor>, Self::Error>>
        + Send
        + Unpin;

    /// Fetch one page of the current state of the world.
    ///
    /// Called with `None` for the first page, then with each token this method returned in
    /// [`ListPage::next_page`] until that is `None`. Unpaginated sources return everything
    /// on the first call with `next_page: None`.
    #[allow(clippy::type_complexity)]
    fn list(
        &self,
        page: Option<Self::PageToken>,
    ) -> impl Future<Output = Result<ListPage<Self::Value, Self::Cursor, Self::PageToken>, Self::Error>> + Send;

    /// Open a stream of changes since `from`.
    ///
    /// If the cursor is too old to resume from, either fail here or emit an error on the
    /// stream — in both cases classified [`ErrorClass::Desync`] — and the driver will
    /// re-list from scratch.
    fn watch(
        &self,
        from: Self::Cursor,
    ) -> impl Future<Output = Result<Self::WatchStream, Self::Error>> + Send;

    /// Classify an error so the driver knows whether to re-list, complain, or just retry.
    fn classify(&self, _err: &Self::Error) -> ErrorClass {
        ErrorClass::Transient
    }

    /// Liveness timeout for the watch stream.
    ///
    /// When `Some`, a watch stream that produces nothing for this long is treated as dead
    /// and reconnected from the last cursor (the analogue of the Kubernetes watcher's idle
    /// timeout for silently dropped connections). `None` (the default) trusts the stream.
    fn idle_timeout(&self) -> Option<Duration> {
        None
    }
}

/// The driver's finite state machine; a transport-neutral mirror of the
/// [`watcher`](crate::watcher()) states.
enum State<S: WatchSource> {
    /// Nothing known; the next step begins a fresh list (emitting [`Event::Init`]).
    Empty,
    /// Draining and paging through the initial list.
    InitPage {
        next_page: Option<S::PageToken>,
        queue: VecDeque<S::Value>,
        cursor: Option<S::Cursor>,
        started: bool,
    },
    /// List complete; the next step opens the watch stream.
    Starting { cursor: S::Cursor },
    /// Streaming changes.
    Watching {
        cursor: S::Cursor,
        stream: S::WatchStream,
    },
}

/// Await the next stream item, treating an idle timeout as end-of-stream (reconnect).
async fn next_or_reconnect<St: Stream + Unpin>(
    stream: &mut St,
    timeout: Option<Duration>,
) -> Option<St::Item> {
    match timeout {
        None => stream.next().await,
        Some(t) => match tokio::time::timeout(t, stream.next()).await {
            Ok(item) => item,
            Err(_elapsed) => {
                debug!(timeout_secs = t.as_secs(), "watch source idle timeout, reconnecting");
                None
            }
        },
    }
}

/// Progress the machine one step: `(Some(event), state)` to emit, `(None, state)` to step again.
#[allow(clippy::too_many_lines)]
async fn step<S: WatchSource>(
    source: &S,
    state: State<S>,
) -> (Option<Result<Event<S::Value>, Error>>, State<S>) {
    match state {
        State::Empty => (Some(Ok(Event::Init)), State::InitPage {
            next_page: None,
            queue: VecDeque::new(),
            cursor: None,
            started: false,
        }),
        State::InitPage {
            next_page,
            mut queue,
            cursor,
            started,
        } => {
            if let Some(obj) = queue.pop_front() {
                return (Some(Ok(Event::InitApply(obj))), State::InitPage {
                    next_page,
                    queue,
                    cursor,
                    started,
                });
            }
            // pages drained and no further page requested: the list is complete
            if started && next_page.is_none() {
                return match cursor {
                    Some(cursor) => (Some(Ok(Event::InitDone)), State::Starting { cursor }),
                    None => (Some(Err(Error::NoResourceVersion)), State::Empty),
                };
            }
            match source.list(next_page).await {
                Ok(page) => (None, State::InitPage {
                    next_page: page.next_page,
                    queue: page.items.into(),
                    cursor: page.cursor.or(cursor),
                    started: true,
                }),
                Err(err) => {
                    if source.classify(&err) == ErrorClass::Denied {
                        warn!("watch source list denied: {err:?}");
                    } else {
                        debug!("watch source list error: {err:?}");
                    }
                    (Some(Err(Error::Source(Box::new(err)))), State::Empty)
                }
            }
        }
        State::Starting { cursor } => match source.watch(cursor.clone()).await {
            Ok(stream) => (None, State::Watching { cursor, stream }),
            Err(err) => {
                let next = match source.classify(&err) {
                    ErrorClass::Desync => State::Empty,
                    ErrorClass::Denied => {
                        warn!("watch source watch denied: {err:?}");
                        State::Starting { cursor }
                    }
                    ErrorClass::Transient => State::Starting { cursor },
                };
                (Some(Err(Error::Source(Box::new(err)))), next)
            }
        },
        State::Watching { cursor, mut stream } => {
            match next_or_reconnect(&mut stream, source.idle_timeout()).await {
                Some(Ok(WatchStep::Apply(obj))) => {
                    (Some(Ok(Event::Apply(obj))), State::Watching { cursor, stream })
                }
                Some(Ok(WatchStep::Delete(obj))) => {
                    (Some(Ok(Event::Delete(obj))), State::Watching { cursor, stream })
                }
                Some(Ok(WatchStep::Cursor(cursor))) => (None, State::Watching { cursor, stream }),
                Some(Err(err)) => {
                    let next = match source.classify(&err) {
                        ErrorClass::Desync => State::Empty,
                        ErrorClass::Denied => {
                            warn!("watch source stream denied: {err:?}");
                            State::Watching { cursor, stream }
                        }
                        ErrorClass::Transient => State::Watching { cursor, stream },
                    };
                    (Some(Err(Error::Source(Box::new(err)))), next)
                }
                // stream ended (or idled out): reconnect from the last cursor
                None => (None, State::Starting { cursor }),
            }
        }
    }
}

/// Watch a [`WatchSource`] continuously, with pagination, resume, and error recovery.
///
/// The generic counterpart of [`watcher`](crate::watcher()): it emits the same
/// [`Event`] protocol (`Init`, `InitApply`Ã—n, `InitDone`, then `Apply`/`Delete`), re-lists on
/// [`ErrorClass::Desync`], reconnects from the last cursor when the stream ends or idles out,
/// and surfaces other errors as [`Error::Source`] while holding its position. Apply backoff
/// downstream exactly as for `watcher`.
pub fn source_watcher<S: WatchSource>(
    source: S,
) -> impl Stream<Item = Result<Event<S::Value>, Error>> {
    futures::stream::unfold((source, State::Empty), |(source, mut state)| async {
        loop {
            match step(&source, state).await {
                (Some(event), next) => return Some((event, (source, next))),
                (None, next) => state = next,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{ErrorClass, ListPage, WatchSource, WatchStep, source_watcher};
    use crate::watcher::{Error, Event};
    use futures::{StreamExt, pin_mut, stream::BoxStream};
    use std::{collections::VecDeque, fmt, sync::Mutex};

    #[derive(Debug)]
    struct TestError(ErrorClass);
    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test error: {:?}", self.0)
        }
    }
    impl std::error::Error for TestError {}

    type Page = ListPage<u32, u64, u32>;
    type Steps = Vec<Result<WatchStep<u32, u64>, TestError>>;

    /// Scripted source: pops one `Page` list result per list call and one `Steps` script per
    /// watch call.
    struct Scripted {
        lists: Mutex<VecDeque<Result<Page, TestError>>>,
        watches: Mutex<VecDeque<Result<Steps, TestError>>>,
    }

    impl WatchSource for Scripted {
        type Value = u32;
        type Cursor = u64;
        type PageToken = u32;
        type Error = TestError;
        type WatchStream = BoxStream<'static, Result<WatchStep<u32, u64>, TestError>>;

        async fn list(&self, _page: Option<u32>) -> Result<Page, TestError> {
            self.lists.lock().unwrap().pop_front().expect("unscripted list call")
        }

        async fn watch(&self, _from: u64) -> Result<Self::WatchStream, TestError> {
            let steps = self
                .watches
                .lock()
                .unwrap()
                .pop_front()
                .expect("unscripted watch call")?;
            Ok(futures::stream::iter(steps).boxed())
        }

        fn classify(&self, err: &TestError) -> ErrorClass {
            err.0
        }
    }

    fn page(items: Vec<u32>, cursor: Option<u64>, next_page: Option<u32>) -> Result<Page, TestError> {
        Ok(ListPage {
            items,
            cursor,
            next_page,
        })
    }

    #[tokio::test]
    async fn emits_init_protocol_across_pages_then_watches() {
        let source = Scripted {
            lists: Mutex::new(VecDeque::from([
                page(vec![1, 2], None, Some(7)),
                page(vec![3], Some(10), None),
            ])),
            watches: Mutex::new(VecDeque::from([Ok(vec![
                Ok(WatchStep::Apply(4)),
                Ok(WatchStep::Cursor(11)),
                Ok(WatchStep::Delete(1)),
            ])])),
        };
        let events = source_watcher(source);
        pin_mut!(events);
        assert!(matches!(events.next().await, Some(Ok(Event::Init))));
        assert!(matches!(events.next().await, Some(Ok(Event::InitApply(1)))));
        assert!(matches!(events.next().await, Some(Ok(Event::InitApply(2)))));
        assert!(matches!(events.next().await, Some(Ok(Event::InitApply(3)))));
        assert!(matches!(events.next().await, Some(Ok(Event::InitDone))));
        assert!(matches!(events.next().await, Some(Ok(Event::Apply(4)))));
        assert!(matches!(events.next().await, Some(Ok(Event::Delete(1)))));
    }

    #[tokio::test]
    async fn desync_on_watch_relists_from_scratch() {
        let source = Scripted {
            lists: Mutex::new(VecDeque::from([
                page(vec![1], Some(10), None),
                page(vec![1, 2], Some(20), None),
            ])),
            watches: Mutex::new(VecDeque::from([
                Err(TestError(ErrorClass::Desync)),
                Ok(vec![Ok(WatchStep::Apply(3))]),
            ])),
        };
        let events = source_watcher(source);
        pin_mut!(events);
        assert!(matches!(events.next().await, Some(Ok(Event::Init))));
        assert!(matches!(events.next().await, Some(Ok(Event::InitApply(1)))));
        assert!(matches!(events.next().await, Some(Ok(Event::InitDone))));
        // desync surfaces the error, then the machine relists
        assert!(matches!(events.next().await, Some(Err(Error::Source(_)))));
        assert!(matches!(events.next().await, Some(Ok(Event::Init))));
        assert!(matches!(events.next().await, Some(Ok(Event::InitApply(1)))));
        assert!(matches!(events.next().await, Some(Ok(Event::InitApply(2)))));
        assert!(matches!(events.next().await, Some(Ok(Event::InitDone))));
        assert!(matches!(events.next().await, Some(Ok(Event::Apply(3)))));
    }

    #[tokio::test]
    async fn stream_end_reconnects_from_cursor() {
        let source = Scripted {
            lists: Mutex::new(VecDeque::from([page(vec![1], Some(10), None)])),
            watches: Mutex::new(VecDeque::from([
                Ok(vec![Ok(WatchStep::Apply(2)), Ok(WatchStep::Cursor(11))]),
                Ok(vec![Ok(WatchStep::Apply(5))]),
            ])),
        };
        let events = source_watcher(source);
        pin_mut!(events);
        assert!(matches!(events.next().await, Some(Ok(Event::Init))));
        assert!(matches!(events.next().await, Some(Ok(Event::InitApply(1)))));
        assert!(matches!(events.next().await, Some(Ok(Event::InitDone))));
        assert!(matches!(events.next().await, Some(Ok(Event::Apply(2)))));
        // first watch stream ends; the driver silently reopens from cursor 11
        assert!(matches!(events.next().await, Some(Ok(Event::Apply(5)))));
    }

    #[tokio::test]
    async fn list_without_cursor_is_an_error_then_restarts() {
        let source = Scripted {
            lists: Mutex::new(VecDeque::from([
                page(vec![1], None, None), // no cursor anywhere: cannot watch
                page(vec![1], Some(10), None),
            ])),
            watches: Mutex::new(VecDeque::from([Ok(vec![])])),
        };
        let events = source_watcher(source);
        pin_mut!(events);
        assert!(matches!(events.next().await, Some(Ok(Event::Init))));
        assert!(matches!(events.next().await, Some(Ok(Event::InitApply(1)))));
        assert!(matches!(
            events.next().await,
            Some(Err(Error::NoResourceVersion))
        ));
        assert!(matches!(events.next().await, Some(Ok(Event::Init))));
    }

    #[tokio::test]
    async fn transient_watch_error_holds_position() {
        let source = Scripted {
            lists: Mutex::new(VecDeque::from([page(vec![1], Some(10), None)])),
            watches: Mutex::new(VecDeque::from([
                Ok(vec![
                    Ok(WatchStep::Apply(2)),
                    Err(TestError(ErrorClass::Transient)),
                    Ok(WatchStep::Apply(3)),
                ]),
            ])),
        };
        let events = source_watcher(source);
        pin_mut!(events);
        assert!(matches!(events.next().await, Some(Ok(Event::Init))));
        assert!(matches!(events.next().await, Some(Ok(Event::InitApply(1)))));
        assert!(matches!(events.next().await, Some(Ok(Event::InitDone))));
        assert!(matches!(events.next().await, Some(Ok(Event::Apply(2)))));
        // transient error surfaces but the same stream continues (no re-list)
        assert!(matches!(events.next().await, Some(Err(Error::Source(_)))));
        assert!(matches!(events.next().await, Some(Ok(Event::Apply(3)))));
    }
}
