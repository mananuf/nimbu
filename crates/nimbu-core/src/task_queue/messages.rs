use std::time::Duration;

use crate::Task;

#[derive(Debug)]
pub enum ExecutionEvent {
    Completed(Task),
    RetryableFailure(Task, String),
    FatalFailure(Task, String),
}

#[derive(Debug)]
pub enum SchedulerCommand {
    Schedule { task: Task, delay: Duration },
    ExecutionResult(ExecutionEvent),
    Shutdown,
}
