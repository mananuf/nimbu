use std::{sync::Arc, time::Duration};

use nimbu_core::{Task, task_queue::TaskQueue};
use tokio::time::sleep;
use tracing::info;

use tracing_subscriber::{fmt, EnvFilter};

pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .init();
}

#[tokio::main]
async fn main() {
    init_tracing();

    info!("starting scheduler demo");

    let queue = Arc::new(TaskQueue::new(10));

    // Immediate task
    let task1 = Task::default();
    queue.enqueue(task1.clone()).await.unwrap();

    // Delayed task
    let task2 = Task::default();
    queue.enqueue_delayed(task2.clone(), Duration::from_secs(3));

    let task3 = Task::default();
    queue.enqueue_delayed(task3.clone(), Duration::from_secs(3));

    info!("tasks enqueued");

    // Consumer loop
    let worker_queue = Arc::clone(&queue);

    tokio::spawn(async move {
        let worker_queue = worker_queue;
        loop {
            if let Some(task) = worker_queue.dequeue().await {
                info!(
                    task_id = ?task.id,
                    "scheduler received task"
                );
            }
        }
    });

    // Let system run
    sleep(Duration::from_secs(10)).await;

    info!("shutting down scheduler");
    queue.shutdown().await;
}
