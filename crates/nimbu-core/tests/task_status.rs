use nimbu_core::{JobId, RetryPolicy, Task, TaskStatus};

#[test]
fn test_valid_transitions() {
    let s = TaskStatus::Pending;
    let s = s.mark_as_assigned().unwrap();
    let s = s.mark_as_running().unwrap();
    let s = s.mark_as_completed().unwrap();
    assert_eq!(s, TaskStatus::Completed);
}

#[test]
fn test_invalid_transition() {
    let s = TaskStatus::Pending;
    assert!(s.mark_as_running().is_err());
}

#[test]
fn test_failed_transition() {
    let s = TaskStatus::Pending;
    // fail only occurs when task is running
    assert!(s.mark_as_failed(3, "error_occured".to_string()).is_err());

    let s = TaskStatus::Running;
    assert!(s.mark_as_failed(3, "error_occured".to_string()).is_ok());
    assert_eq!(
        s.mark_as_failed(3, "error_occured".to_string()).unwrap(),
        TaskStatus::Failed {
            attempt: 3,
            error: "error_occured".to_string()
        }
    );
}

#[test]
fn test_failed_permanent_transition() {
    let s = TaskStatus::Pending;
    // fail only occurs when task is running
    assert!(
        s.mark_as_failed_permanent("error_occured".to_string())
            .is_err()
    );

    // fails permanently if running
    let s = TaskStatus::Running;
    assert!(
        s.mark_as_failed_permanent("error_occured".to_string())
            .is_ok()
    );
    assert_eq!(
        s.mark_as_failed_permanent("error_occured".to_string())
            .unwrap(),
        TaskStatus::FailedPermanent {
            error: "error_occured".to_string()
        }
    );

    // fails permananetly if already in failed state
    let s = TaskStatus::Failed {
        attempt: 3,
        error: "error".into(),
    };
    assert!(
        s.mark_as_failed_permanent("error_occured".to_string())
            .is_ok()
    );
    assert_eq!(
        s.mark_as_failed_permanent("error_occured".to_string())
            .unwrap(),
        TaskStatus::FailedPermanent {
            error: "error_occured".to_string()
        }
    );
}

#[test]
fn test_valid_transitions_with_retry_policy() {
    let mut t = Task::builder(vec![])
        .retry_policy(
            RetryPolicy::builder()
                .max_retries(3)
                .backoff_ms(100)
                .build(),
        )
        .build();

    t.assign().unwrap();
    t.start().unwrap();
    t.complete().unwrap();

    assert_eq!(t.status, TaskStatus::Completed);
}

#[test]
fn test_retry_flow() {
    let job = JobId::new();
    let mut t = Task::builder(vec![])
        .job_id(job)
        .retry_policy(
            RetryPolicy::builder()
                .max_retries(2)
                .backoff_ms(100)
                .build(),
        )
        .build();

    t.assign().unwrap();
    t.start().unwrap();

    t.fail_retry("err").unwrap();
    match &t.status {
        TaskStatus::Failed { attempt, .. } => assert_eq!(*attempt, 1),
        _ => panic!("wrong state"),
    }
}

#[test]
fn test_retry_limit_exceeded() {
    let mut t = Task::builder(vec![])
        .retry_policy(RetryPolicy::builder().max_retries(1).build())
        .build();

    t.assign().unwrap();
    t.start().unwrap();
    t.fail_retry("err").unwrap();

    t.start().unwrap_err();
    assert!(t.fail_retry("again").is_err());
}
