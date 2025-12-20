use std::time::Duration;

use nimbu_core::{Task, task_queues::TaskQueue};
use tokio::time::{advance, pause, timeout};

#[tokio::test]
async fn fifo_enqueue_dequeue() {
    let mut queue = TaskQueue::new(10);

    let t1 = Task::default();
    let t2 = Task::default();

    queue.enqueue(t1.clone()).await.unwrap();
    queue.enqueue(t2.clone()).await.unwrap();

    let r1 = queue.dequeue().await.unwrap();
    let r2 = queue.dequeue().await.unwrap();

    assert_eq!(r1.id, t1.id);
    assert_eq!(r2.id, t2.id);
}

#[tokio::test]
async fn capacity_backpressure_blocks() {
    let mut queue = TaskQueue::new(1);
    queue.enqueue(Task::default()).await.unwrap();

    let fut = queue.enqueue(Task::default());
    assert!(timeout(Duration::from_millis(50), fut).await.is_err());

    queue.dequeue().await.unwrap();
}

#[tokio::test]
async fn delayed_enqueue_is_deterministic() {
    pause();

    let mut queue = TaskQueue::new(5);
    let task = Task::default();
    let id = task.clone().id;

    queue.enqueue_delayed(task, Duration::from_secs(10));

    // assert_eq!(queue.len(), 0);

    advance(Duration::from_secs(9)).await;
    assert!(queue.dequeue().await.is_none());

    advance(Duration::from_secs(1)).await;
    let out = queue.dequeue().await.unwrap();
    assert_eq!(out.id, id);
}

#[tokio::test]
async fn shutdown_stops_queue() {
    let queue = TaskQueue::new(5);
    queue.shutdown().await;
}
