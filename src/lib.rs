use std::{
    any::Any,
    future::Future,
    sync::{Arc, Mutex, OnceLock, Weak},
};

use thiserror::Error;

/// Represents the logic of a process.
pub trait Process: Sized + Send + 'static {
    type Message: Send + 'static;
    type Output: std::fmt::Debug + Send + Sync + 'static;
    const MAILBOX_CAP: usize = 1;

    /// The "main function" of the process.
    fn run(&mut self, mailbox: &mut Mailbox<Self>) -> impl Future<Output = Self::Output> + Send;

    /// Spawn a process using a given processor.
    fn spawn(mut self, processor: impl Processor) -> Handle<Self> {
        let (send, recv) = tachyonix::channel(Self::MAILBOX_CAP);
        let output = Arc::new(OnceLock::new());
        let death_event = Arc::new(async_event::Event::new());
        let handle = Handle {
            send,
            output: output.clone(),
            death_event: death_event.clone(),

            _to_drop: Arc::new(Mutex::new(Box::new(0))),
        };
        let mut mailbox = Mailbox {
            recv,
            handle: handle.downgrade(),
        };
        let task = processor.spawn_future(async move {
            let out = self.run(&mut mailbox).await;
            output.set(out).unwrap();
            death_event.notify_all();
            drop(mailbox); // ensure dropping *after* output is set
        });
        *handle._to_drop.lock().unwrap() = Box::new(task);
        handle
    }

    /// Convenience method to spawn onto the smolscale executor.
    #[cfg(feature = "smolscale")]
    fn spawn_smolscale(self) -> Handle<Self> {
        self.spawn(SscaleProcessor)
    }
}

/// A mailbox for receiving messages addressed to a particular process.
pub struct Mailbox<P: Process> {
    recv: tachyonix::Receiver<P::Message>,

    handle: WeakHandle<P>,
}

impl<P: Process> Mailbox<P> {
    pub async fn recv(&mut self) -> P::Message {
        match self.recv.recv().await {
            Ok(val) => val,
            Err(_) => futures_util::future::pending().await,
        }
    }

    pub fn handle(&self) -> WeakHandle<P> {
        self.handle.clone()
    }
}

/// A handle to a running process. If all Handles to a process are dropped, the process will be dropped.
pub struct Handle<P: Process> {
    send: tachyonix::Sender<P::Message>,
    output: Arc<OnceLock<P::Output>>,
    death_event: Arc<async_event::Event>,

    _to_drop: Arc<Mutex<Box<dyn Any + Send + Sync>>>,
}

impl<P: Process> Clone for Handle<P> {
    fn clone(&self) -> Self {
        Self {
            send: self.send.clone(),
            output: self.output.clone(),
            death_event: self.death_event.clone(),

            _to_drop: self._to_drop.clone(),
        }
    }
}

impl<P: Process> Handle<P> {
    /// Sends a message to the process.
    pub async fn send(&self, msg: P::Message) -> Result<(), SendError> {
        match self.send.send(msg).await {
            Ok(_) => Ok(()),
            Err(_) => Err(SendError::ProcessStopped),
        }
    }

    /// Sends a message to the process, or drop it if the mailbox is full.
    pub async fn send_or_drop(&self, msg: P::Message) -> Result<(), SendError> {
        match self.send.try_send(msg) {
            Err(tachyonix::TrySendError::Closed(_)) => Err(SendError::ProcessStopped),
            _ => Ok(()),
        }
    }

    /// Gets the output value of the process if it has terminated.
    pub fn output(&self) -> Option<&P::Output> {
        self.output.get()
    }

    /// Waits for the the output value of the process].
    pub async fn wait(&self) -> &P::Output {
        self.death_event.wait_until(|| self.output.get()).await
    }

    /// Downgrades the Handle to a WeakHandle.
    pub fn downgrade(&self) -> WeakHandle<P> {
        WeakHandle {
            send: self.send.clone(),
            output: self.output.clone(),
            death_event: self.death_event.clone(),
            _to_drop: Arc::downgrade(&self._to_drop),
        }
    }
}

/// A "weak" handle to a process, which does not keep it running unless there are Handles to it.
pub struct WeakHandle<P: Process> {
    send: tachyonix::Sender<P::Message>,
    output: Arc<OnceLock<P::Output>>,
    death_event: Arc<async_event::Event>,

    _to_drop: Weak<Mutex<Box<dyn Any + Send + Sync>>>,
}

impl<P: Process> Clone for WeakHandle<P> {
    fn clone(&self) -> Self {
        Self {
            send: self.send.clone(),
            output: self.output.clone(),
            death_event: self.death_event.clone(),

            _to_drop: self._to_drop.clone(),
        }
    }
}

impl<P: Process> WeakHandle<P> {
    /// Sends a message to the process.
    pub async fn send(&self, msg: P::Message) -> Result<(), SendError> {
        match self.send.send(msg).await {
            Ok(_) => Ok(()),
            Err(_) => Err(SendError::ProcessStopped),
        }
    }

    /// Sends a message to the process, or drop if the mailbox is full.
    pub async fn send_or_drop(&self, msg: P::Message) -> Result<(), SendError> {
        match self.send.try_send(msg) {
            Err(tachyonix::TrySendError::Closed(_)) => Err(SendError::ProcessStopped),
            _ => Ok(()),
        }
    }

    /// Gets the output value of the process if it has terminated.
    pub fn output(&self) -> Option<&P::Output> {
        self.output.get()
    }

    /// Attempts to upgrade the WeakHandle to a Handle.
    pub fn upgrade(&self) -> Option<Handle<P>> {
        Some(Handle {
            send: self.send.clone(),
            output: self.output.clone(),
            death_event: self.death_event.clone(),
            _to_drop: self._to_drop.upgrade()?,
        })
    }
}

impl<P, T, U> Handle<P>
where
    P: Process<Message = Request<T, U>>,
{
    pub async fn request(&self, req: T) -> Result<U, RequestError> {
        let (respond, recv_response) = oneshot::channel();
        let req = Request {
            inner: req,
            respond,
        };
        self.send(req).await.map_err(RequestError::SendFailed)?;
        match recv_response.await {
            Ok(val) => Ok(val),
            Err(_) => Err(RequestError::RequestRefused),
        }
    }
}

impl<P, T, U> WeakHandle<P>
where
    P: Process<Message = Request<T, U>>,
{
    pub async fn request(&self, req: T) -> Result<U, RequestError> {
        let (respond, recv_response) = oneshot::channel();
        let req = Request {
            inner: req,
            respond,
        };
        self.send(req).await.map_err(RequestError::SendFailed)?;
        match recv_response.await {
            Ok(val) => Ok(val),
            Err(_) => Err(RequestError::RequestRefused),
        }
    }
}

/// A request. This cannot be manually constructed, but should be used in conjunction with `send_request`.
pub struct Request<T, U> {
    inner: T,
    respond: oneshot::Sender<U>,
}

impl<T, U> Request<T, U> {
    /// Splits this request into the inner value, as well as a function for responding to the request.
    pub fn take(self) -> (T, impl FnOnce(U)) {
        (self.inner, move |u| {
            let _ = self.respond.send(u);
        })
    }
}

#[derive(Error, Debug, Clone)]
/// Error for doing a request/response cycle to a process.
pub enum RequestError {
    #[error("could not send to process: {0:?}")]
    SendFailed(SendError),
    #[error("process refused to respond")]
    RequestRefused,
}

#[derive(Error, Debug, Clone)]
/// Error for sending into a handle.
pub enum SendError {
    #[error("process stopped")]
    ProcessStopped,
    #[error("process is in the process of stopping")]
    ProcessStopping,
}

/// An executor of processes.
pub trait Processor {
    /// Spawns an arbitrary future into the background. The returned handle **must** cancel the future when dropped.
    fn spawn_future(
        &self,
        fut: impl Future<Output = ()> + Send + 'static,
    ) -> impl Any + Send + Sync;
}

impl<T: Processor> Processor for &T {
    fn spawn_future(
        &self,
        fut: impl Future<Output = ()> + Send + 'static,
    ) -> impl Any + Send + Sync {
        (*self).spawn_future(fut)
    }
}

/// A [Processor] implemented using the smolscale executor.
#[cfg(feature = "smolscale")]
pub struct SscaleProcessor;

#[cfg(feature = "smolscale")]
impl Processor for SscaleProcessor {
    fn spawn_future(
        &self,
        fut: impl Future<Output = ()> + Send + 'static,
    ) -> impl Any + Send + Sync {
        smolscale::spawn(fut)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    struct TestProcess;

    impl Process for TestProcess {
        type Message = String;
        type Output = i32;

        async fn run(&mut self, mailbox: &mut Mailbox<Self>) -> Self::Output {
            let msg = mailbox.recv().await;
            assert_eq!(msg, "Hello");
            42
        }
    }

    #[test]
    #[cfg(feature = "smolscale")]
    fn test_process_spawn_and_message() {
        smolscale::block_on(async {
            let handle = TestProcess.spawn(SscaleProcessor);

            // Send a message to the process
            handle.send("Hello".to_string()).await.unwrap();

            std::thread::sleep(Duration::from_millis(100));

            // Check the output
            assert_eq!(handle.output(), Some(&42));
        });
    }
}
