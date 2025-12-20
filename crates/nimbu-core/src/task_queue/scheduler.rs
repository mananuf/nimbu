use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_util::time::DelayQueue;
use tracing::{error, info, warn};

use crate::{
    Task,
    task_queue::{
        backoff::compute_backoff,
        messages::{ExecutionEvent, SchedulerCommand},
    },
};

pub async fn scheduler_loop(
    ready_tx: mpsc::Sender<Task>,
    mut cmd_rx: mpsc::Receiver<SchedulerCommand>,
) {
    let mut delay_queue = DelayQueue::<Task>::new();

    info!("delay scheduler started");

    loop {
        tokio::select! {
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    SchedulerCommand::Schedule { task, delay } => {
                        delay_queue.insert(task, delay);
                    }

                    SchedulerCommand::ExecutionResult(event) => {
                        handle_execution_event(event, &mut delay_queue);
                    }

                    SchedulerCommand::Shutdown => break,
                }
            }

            Some(expired) = delay_queue.next() => {
                let task = expired.into_inner();
                let _ = ready_tx.send(task).await;
            }

            else => {
                break;
            }
        }
    }

    info!("delay scheduler exited");
}

fn handle_execution_event(event: ExecutionEvent, delay_queue: &mut DelayQueue<Task>) {
    match event {
        ExecutionEvent::Completed(mut task) => {
            let _ = task.complete();
            info!(
                task_id = ?task.id,
                "task completed successfully"
            );
        }

        ExecutionEvent::RetryableFailure(mut task, error) => {
            task.mark_retryable_failure(error);

            if let Some(policy) = &task.retry_policy {
                if policy.can_retry(task.attempts) {
                    if let Some(delay) = compute_backoff(&task) {
                        warn!(
                            task_id = ?task.id,
                            attempts = task.attempts,
                            delay_ms = delay.as_millis(),
                            "scheduling retry"
                        );
                        delay_queue.insert(task, delay);
                        return;
                    }
                }
            }

            task.mark_permanent_failure("retry limit exceeded".into());
            error!(
                task_id = ?task.id,
                "task permanently failed"
            );
        }

        ExecutionEvent::FatalFailure(mut task, error) => {
            task.mark_permanent_failure(error);
            error!(
                task_id = ?task.id,
                "fatal task failure"
            );
        }
    }
}
