use agent_session_router::protocol::{
    PROTOCOL_VERSION, RouterErrorCode, ServerMessage, WorkspaceName, parse_client_message,
    parse_server_message,
};

const TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
fn parse_error(raw: &str) -> RouterErrorCode {
    parse_client_message(raw)
        .err()
        .expect("message must be rejected")
}

#[test]
fn protocol_version_is_checked_before_message_shape() {
    let raw = r#"{"type":"register","protocolVersion":1}"#;
    assert_eq!(parse_error(raw), RouterErrorCode::ProtocolMismatch);
}

#[test]
fn registration_rejects_unknown_and_inconsistent_agent_fields() {
    let unknown = format!(
        r#"{{"type":"register","protocolVersion":{PROTOCOL_VERSION},"agent":{{"agentId":"worker-1","side":"generic","client":"omp","surprise":true}},"token":"{TOKEN}","delegationToken":null}}"#
    );
    assert_eq!(parse_error(&unknown), RouterErrorCode::InvalidMessage);

    let inconsistent = format!(
        r#"{{"type":"register","protocolVersion":{PROTOCOL_VERSION},"agent":{{"agentId":"worker-1","side":"claude","client":"codex-cli"}},"token":"{TOKEN}","delegationToken":null}}"#
    );
    assert_eq!(parse_error(&inconsistent), RouterErrorCode::InvalidMessage);
}

#[test]
fn workspace_names_are_validated_during_deserialization() {
    assert!(serde_json::from_str::<WorkspaceName>(r#""valid-name""#).is_ok());
    assert!(serde_json::from_str::<WorkspaceName>(r#""../escape""#).is_err());
    assert!(serde_json::from_str::<WorkspaceName>(r#""UPPER""#).is_ok());
    assert!(serde_json::from_str::<WorkspaceName>(r#""-leading""#).is_err());
}

#[test]
fn shared_content_obeys_raw_and_encoded_limits() {
    let escaped = "\n".repeat(64 * 1024);
    let raw = serde_json::json!({
        "type": "workspace_post",
        "requestId": "post-1",
        "content": escaped,
    })
    .to_string();
    assert_eq!(parse_error(&raw), RouterErrorCode::InvalidMessage);
}

#[test]
fn reply_shape_is_unambiguous() {
    let success_with_error = r#"{"type":"reply","requestId":"reply-1","ok":true,"content":"done","error":"provider_error"}"#;
    assert_eq!(
        parse_error(success_with_error),
        RouterErrorCode::InvalidMessage
    );

    let failure_with_content = r#"{"type":"reply","requestId":"reply-2","ok":false,"content":"leak","error":"provider_error"}"#;
    assert_eq!(
        parse_error(failure_with_content),
        RouterErrorCode::InvalidMessage
    );
}

#[test]
fn valid_registration_is_accepted() {
    let raw = format!(
        r#"{{"type":"register","protocolVersion":{PROTOCOL_VERSION},"agent":{{"agentId":"worker-1","side":"generic","client":"omp"}},"token":"{TOKEN}","delegationToken":null}}"#
    );
    assert!(parse_client_message(&raw).is_ok());
}

#[test]
fn external_operation_shapes_are_strict_and_semantic() {
    let operation_id = uuid::Uuid::new_v4();
    let valid_link = serde_json::json!({
        "type": "task_link",
        "requestId": "link-1",
        "workspace": "room-a",
        "operationId": operation_id,
        "taskId": 1,
        "expectedVersion": 1,
        "provider": "github",
        "externalId": "17",
        "replace": false
    })
    .to_string();
    assert!(parse_client_message(&valid_link).is_ok());

    let missing_report = serde_json::json!({
        "type": "task_publish",
        "requestId": "publish-1",
        "workspace": "room-a",
        "operationId": operation_id,
        "taskId": 1,
        "expectedVersion": 1,
        "provider": "linear",
        "kind": "report",
        "reportId": null
    })
    .to_string();
    assert_eq!(
        parse_error(&missing_report),
        RouterErrorCode::InvalidMessage
    );

    let inconsistent_resolution = serde_json::json!({
        "type": "task_external_resolve",
        "requestId": "resolve-1",
        "workspace": "room-a",
        "operationId": operation_id,
        "resolutionId": uuid::Uuid::new_v4(),
        "outcome": "not_applied",
        "externalId": "must-not-be-present",
        "note": "operator evidence"
    })
    .to_string();
    assert_eq!(
        parse_error(&inconsistent_resolution),
        RouterErrorCode::InvalidMessage
    );

    let unknown_reload_field =
        r#"{"type":"integration_reload","requestId":"reload-1","surprise":true}"#;
    assert_eq!(
        parse_error(unknown_reload_field),
        RouterErrorCode::InvalidMessage
    );
}

#[test]
fn task_attempt_change_preserves_correlated_attempt_payload() {
    let attempt_id = uuid::Uuid::new_v4();
    let session_id = uuid::Uuid::new_v4();
    let raw = serde_json::json!({
        "type": "task_attempt_changed",
        "workspace": "room-a",
        "taskId": 7,
        "attempt": {
            "id": attempt_id,
            "taskId": 7,
            "agentId": "worker-1",
            "sessionId": session_id,
            "workRequestId": "work-1",
            "resumedFromCheckpointId": null,
            "status": "running",
            "stopEvidence": "unknown",
            "reason": null,
            "startedAt": 12,
            "endedAt": null,
            "stoppedAt": null
        },
        "closedAttemptId": null,
        "current": {
            "taskId": 7,
            "attemptId": attempt_id
        },
        "stopPending": null
    })
    .to_string();

    let parsed = parse_server_message(&raw).expect("valid attempt notification");
    let ServerMessage::TaskAttemptChanged {
        workspace,
        task_id,
        attempt: Some(attempt),
        closed_attempt_id,
        current: Some(current),
        stop_pending,
    } = parsed
    else {
        panic!("unexpected attempt notification");
    };
    assert_eq!(workspace.as_str(), "room-a");
    assert_eq!(task_id, 7);
    assert_eq!(attempt.id, attempt_id);
    assert_eq!(attempt.session_id, session_id);
    assert_eq!(current.attempt_id, attempt_id);
    assert_eq!(closed_attempt_id, None);
    assert_eq!(stop_pending, None);

    let mut with_unknown = serde_json::from_str::<serde_json::Value>(&raw).expect("JSON");
    with_unknown["unexpected"] = serde_json::Value::Bool(true);
    assert!(matches!(
        parse_server_message(&with_unknown.to_string()),
        Err(RouterErrorCode::InvalidMessage)
    ));
}

#[test]
fn task_history_response_is_typed_and_strict() {
    let raw = serde_json::json!({
        "type": "task_history",
        "requestId": "history-1",
        "workspace": "room-a",
        "taskId": 7,
        "events": [{
            "seq": 4,
            "actorId": "operator:admin",
            "createdAt": 10,
            "change": "created",
            "task": {
                "id": 7,
                "workspace": "room-a",
                "title": "Typed history",
                "state": "todo",
                "version": 1,
                "assignedAgentId": null,
                "currentAttemptId": null,
                "lastExecutorId": null,
                "executionSessionId": null,
                "lastCheckpointAt": null,
                "pauseReason": null,
                "stopEvidence": null,
                "createdAt": 10,
                "updatedAt": 10
            },
            "attemptId": null,
            "reportId": null,
            "externalOperationId": null
        }],
        "nextCursor": 4,
        "hasMore": false
    });
    let parsed = parse_server_message(&raw.to_string()).expect("valid typed task history");
    let ServerMessage::TaskHistory { page, .. } = parsed else {
        panic!("unexpected task history response");
    };
    assert_eq!(page.task_id, 7);
    assert_eq!(page.events[0].event.task.title, "Typed history");

    let mut mismatched = raw.clone();
    mismatched["events"][0]["task"]["id"] = serde_json::json!(8);
    assert_eq!(
        parse_server_message(&mismatched.to_string())
            .err()
            .expect("mismatched task must fail"),
        RouterErrorCode::InvalidMessage
    );
    let mut unknown = raw;
    unknown["surprise"] = serde_json::json!(true);
    assert_eq!(
        parse_server_message(&unknown.to_string())
            .err()
            .expect("unknown field must fail"),
        RouterErrorCode::InvalidMessage
    );
}
