#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("queue is full")]
    Full,

    #[error("queue is closed")]
    Closed,
}
