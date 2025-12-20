use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};
use tracing::{debug, info};

use crate::{
    Task,
    task_queue::{messages::SchedulerCommand, scheduler::scheduler_loop},
};

#[derive(Debug)]
pub struct TaskQueue {
    pub ready_tx: mpsc::Sender<Task>,
    pub ready_rx: Mutex<mpsc::Receiver<Task>>,

    pub scheduler_tx: mpsc::Sender<SchedulerCommand>,
    pub scheduler_handle: JoinHandle<()>,

    pub len: AtomicUsize,
    pub capacity: usize,
}

impl TaskQueue {
    pub fn new(capacity: usize) -> Self {
        info!(capacity, "initializing task queue");

        let (ready_tx, ready_rx) = mpsc::channel(capacity);
        let (scheduler_tx, scheduler_rx) = mpsc::channel(1024);

        let scheduler_handle = tokio::spawn(scheduler_loop(ready_tx.clone(), scheduler_rx));

        info!(capacity, "task queue initialized");

        Self {
            ready_tx,
            ready_rx: Mutex::new(ready_rx),
            scheduler_tx,
            scheduler_handle,
            len: AtomicUsize::new(0),
            capacity,
        }
    }

    pub async fn enqueue(&self, task: Task) -> Result<(), ()> {
        self.ready_tx.send(task).await.map_err(|_| ())?;
        self.len.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn enqueue_delayed(&self, task: Task, delay: Duration) {
        debug!(
            task_id = ?task.id,
            delay_ms = delay.as_millis(),
            "enqueue delayed task"
        );

        let _ = self
            .scheduler_tx
            .try_send(SchedulerCommand::Schedule { task, delay });
    }

    pub async fn dequeue(&self) -> Option<Task> {
        let mut rx = self.ready_rx.lock().await;
        let task = rx.recv().await;

        if task.is_some() {
            self.len.fetch_sub(1, Ordering::SeqCst);
        }

        task
    }

    pub async fn shutdown(self) {
        info!("task queue shutdown initiated");

        let _ = self.scheduler_tx.send(SchedulerCommand::Shutdown).await;
        let _ = self.scheduler_handle.await;

        info!("task queue shutdown complete");
    }
}
