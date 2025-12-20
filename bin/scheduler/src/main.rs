use std::{sync::Arc, time::Duration};

use nimbu_core::{Task, task_queue::queue::TaskQueue};
use tokio::time::sleep;
use tracing::info;

use tracing_subscriber::{EnvFilter, fmt};

pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

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
    let task1 = Task::new(vec![1, 2, 3, 4, 5]).build();
    queue.enqueue(task1.clone()).await.unwrap();

    // Delayed tasks
    let task2 = Task::new(vec![6, 7, 8, 9, 10]).build();
    queue.enqueue_delayed(task2.clone(), Duration::from_secs(3));

    let task3 = Task::new(vec![11, 12, 13, 14, 15]).build();
    queue.enqueue_delayed(task3.clone(), Duration::from_secs(3));

    info!("tasks enqueued");

    // Watch channel to signal worker shutdown
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    let worker_queue = queue.clone();
    let worker_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                res = shutdown_rx.changed() => {
                    if res.is_ok() && *shutdown_rx.borrow() {
                        info!("worker shutting down");
                        break;
                    }
                }

                task_opt = worker_queue.dequeue() => {
                    if let Some(task) = task_opt {
                        info!(task_id = ?task.id, "worker received task");
                    } else {
                        // optional: small delay to avoid busy-loop when queue empty
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }
    });

    // Let system run for demo
    sleep(Duration::from_secs(10)).await;

    info!("shutting down scheduler");

    // signal worker to stop
    let _ = shutdown_tx.send(true);
    let _ = worker_handle.await;

    let queue = Arc::try_unwrap(queue).expect("error stopping worker queue");

    // shutdown scheduler
    queue.shutdown().await;

    info!("scheduler demo complete");
}
