use std::fmt::Display;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Assigned,
    Running,
    Completed,
    Failed { attempt: u32, error: String },
    FailedPermanent { error: String },
}

#[derive(Debug, Error)]
pub enum TaskTransitionError {
    #[error("illegal task transition from {from} to {to}")]
    Illegal {
        from: &'static str,
        to: &'static str,
    },
}

impl Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Assigned => write!(f, "Assigned"),
            TaskStatus::Pending => write!(f, "Pending"),
            TaskStatus::Running => write!(f, "Running"),
            TaskStatus::Completed => write!(f, "Completed"),
            TaskStatus::Failed { attempt, error } => {
                write!(f, "Failed(attempt={}, error={})", attempt, error)
            }
            TaskStatus::FailedPermanent { error } => write!(f, "FailedPerananent(error={})", error),
        }
    }
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "Pending",
            TaskStatus::Assigned => "Assigned",
            TaskStatus::Running => "Running",
            TaskStatus::Completed => "Completed",
            TaskStatus::Failed { .. } => "Failed",
            TaskStatus::FailedPermanent { .. } => "FailedPermanent",
        }
    }

    pub fn mark_as_assigned(&self) -> Result<TaskStatus, TaskTransitionError> {
        match self {
            TaskStatus::Pending => Ok(TaskStatus::Assigned),
            status => Err(TaskTransitionError::Illegal {
                from: status.as_str(),
                to: "Assigned",
            }),
        }
    }

    pub fn mark_as_running(&self) -> Result<TaskStatus, TaskTransitionError> {
        match self {
            TaskStatus::Assigned => Ok(TaskStatus::Running),
            status => Err(TaskTransitionError::Illegal {
                from: status.as_str(),
                to: "Running",
            }),
        }
    }

    pub fn mark_as_completed(&self) -> Result<TaskStatus, TaskTransitionError> {
        match self {
            TaskStatus::Running => Ok(TaskStatus::Completed),
            status => Err(TaskTransitionError::Illegal {
                from: status.as_str(),
                to: "Completed",
            }),
        }
    }

    pub fn mark_as_failed(
        &self,
        attempt: u32,
        error: String,
    ) -> Result<TaskStatus, TaskTransitionError> {
        match self {
            TaskStatus::Running => Ok(TaskStatus::Failed { attempt, error }),
            status => Err(TaskTransitionError::Illegal {
                from: status.as_str(),
                to: "Failed",
            }),
        }
    }

    pub fn mark_as_failed_permanent(
        &self,
        error: String,
    ) -> Result<TaskStatus, TaskTransitionError> {
        match self {
            TaskStatus::Running | TaskStatus::Failed { .. } => Ok(TaskStatus::FailedPermanent { error }),
            status => Err(TaskTransitionError::Illegal {
                from: status.as_str(),
                to: "FailedPermanent",
            }),
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Completed | TaskStatus::FailedPermanent { .. }
        )
    }

    pub fn can_retry(&self, max_attempts: u32) -> bool {
        match self {
            TaskStatus::Failed { attempt, .. } => *attempt < max_attempts,
            _ => false,
        }
    }
}
