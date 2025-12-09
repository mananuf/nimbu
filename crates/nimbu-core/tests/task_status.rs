use nimbu_core::TaskStatus;

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
    let s = TaskStatus::Failed { attempt: 3, error: "error".into() };
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
