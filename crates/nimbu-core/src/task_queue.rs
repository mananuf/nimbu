use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::{Notify, mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::time::DelayQueue;
use tracing::{debug, info, warn};

use crate::Task;

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("queue is full")]
    Full,

    #[error("queue is closed")]
    Closed,
}

#[derive(Debug)]
struct ScheduledTask {
    task: Task,
}

#[derive(Debug)]
pub struct TaskQueue {
    sender: Arc<Mutex<Option<mpsc::Sender<Task>>>>,
    receiver: Mutex<mpsc::Receiver<Task>>,
    capacity: usize,
    len: Arc<AtomicUsize>,
    shutdown: Arc<Notify>,
    delay_handle: Mutex<Option<JoinHandle<()>>>,
}

impl TaskQueue {
    pub fn new(capacity: usize) -> Self {
        info!(capacity, "initializing task queue");

        let (tx, rx) = mpsc::channel(capacity);
        let len = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(Notify::new());

        let delay_tx = tx.clone();
        let delay_len = len.clone();
        let shutdown_signal = shutdown.clone();

        let delay_handle = tokio::spawn(async move {
            info!("delay scheduler started");
            let mut delay_queue: DelayQueue<ScheduledTask> = DelayQueue::new();

            loop {
                tokio::select! {
                    _ = shutdown_signal.notified() => {
                        info!("delay scheduler shutting down");
                        break;
                    }
                    Some(expired) = delay_queue.next() => {
                        let task = expired.into_inner().task;
                        if delay_tx.send(task).await.is_ok() {
                            delay_len.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }

            info!("delay scheduler exited");
        });

        Self {
            sender: Arc::new(Mutex::new(Some(tx))),
            receiver: Mutex::new(rx),
            capacity,
            len,
            shutdown,
            delay_handle: Mutex::new(Some(delay_handle)),
        }
    }

    pub async fn enqueue(&self, task: Task) -> Result<(), QueueError> {
        debug!(task_id = ?task.id, "enqueue immediate task");

        let sender = self.sender.lock().await;
        let sender = sender.as_ref().ok_or(QueueError::Closed)?;

        sender.send(task).await.map_err(|_| {
            warn!("enqueue failed: queue closed");
            QueueError::Closed
        })?;

        self.len.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn try_enqueue(&self, task: Task) -> Result<(), QueueError> {
        let sender = self.sender.try_lock().ok().and_then(|s| s.clone());

        let sender = sender.ok_or(QueueError::Closed)?;

        match sender.try_send(task) {
            Ok(()) => {
                self.len.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => Err(QueueError::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(QueueError::Closed),
        }
    }

    pub fn enqueue_delayed(&self, task: Task, delay: Duration) {
        info!(
            task_id = ?task.id,
            delay_ms = delay.as_millis(),
            "enqueue delayed task (independent delay task)"
        );

        let shutdown = self.shutdown.clone();
        let sender = self.sender.clone();
        let len = self.len.clone();

        tokio::spawn(async move {
            let mut dq = DelayQueue::new();
            dq.insert(ScheduledTask { task }, delay);

            tokio::select! {
                _ = shutdown.notified() => {
                    debug!("delayed task cancelled due to shutdown");
                }
                Some(expired) = dq.next() => {
                    let task = expired.into_inner().task;
                    if let Some(sender) = sender.lock().await.as_ref() {
                        if sender.send(task).await.is_ok() {
                            len.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }
        });
    }

    pub async fn dequeue(&self) -> Option<Task> {
        let mut receiver = self.receiver.lock().await;
        let task = receiver.recv().await;

        if let Some(ref task) = task {
            debug!(task_id = ?task.id, "task dequeued");
            self.len.fetch_sub(1, Ordering::SeqCst);
        } else {
            debug!("receiver closed");
        }

        task
    }

    pub async fn shutdown(&self) {
        info!("task queue shutdown initiated");

        self.shutdown.notify_waiters();

        self.sender.lock().await.take();

        if let Some(handle) = self.delay_handle.lock().await.take() {
            let _ = handle.await;
        }

        info!("task queue shutdown complete");
    }
}
