use agent_session_router::{
    protocol::{
        ClientMessage, HistoryPage, WorkspaceEvent, WorkspaceEventKind, WorkspaceName,
        WorkspaceSummary,
    },
    tasks::{
        AttemptStatus, StopEvidence, TaskAttempt, TaskChange, TaskDetail, TaskEvent,
        TaskMutationResult, TaskState, TaskSummary,
    },
    tui::{
        UiCommand, UiInput,
        input::{command_not_sent, handle, request_detach},
        state::{
            Connection, DetailKind, EventDisposition, FieldId, Focus, FormKind, FormState, Header,
            HistoryMode, InputBuffer, MAX_EVENT_BYTES, MAX_HISTORY_EVENTS, MAX_LIVE_EVENTS, Modal,
            MutationStage, Ownership, PreviousHistoryLoad, RequestStage, StateError, Tab,
            TaskFilter, UiState, WorkspaceStamp,
        },
        view::{display_endpoint, render},
    },
};
use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::{Terminal, backend::TestBackend, buffer::Buffer, style::Color};
use unicode_width::UnicodeWidthStr;
use uuid::Uuid;

fn workspace(name: &str) -> WorkspaceName {
    WorkspaceName::parse(name).unwrap()
}

fn state() -> UiState {
    let mut state = UiState::new(
        Header {
            profile: Some("office".to_owned()),
            endpoint: "ws://127.0.0.1:8787/ws".to_owned(),
            transport: "local".to_owned(),
            ownership: Ownership::Owned,
            instance_id: Some(Uuid::from_u128(1)),
            is_admin: true,
        },
        false,
    );
    state.resize(120, 35);
    state.connection_changed(Connection::Connected { epoch: 1 });
    state.commit_workspace(workspace("room"));
    let stamp = state.stamp().unwrap();
    let ticket = state.chat.begin_history_load(stamp.clone());
    state.apply_recent_page(&ticket, page(1, 0)).unwrap();
    assert!(state.subscription_live(&stamp, 0));
    state
}

fn event(seq: i64, content: impl Into<String>) -> WorkspaceEvent {
    WorkspaceEvent {
        workspace: workspace("room"),
        seq,
        kind: WorkspaceEventKind::Chat,
        actor_id: "operator:admin".to_owned(),
        created_at: 0,
        request_id: None,
        target_id: None,
        task_id: None,
        content: Some(content.into()),
        ok: None,
        error: None,
    }
}

fn page(first: i64, last: i64) -> HistoryPage {
    HistoryPage {
        workspace: workspace("room"),
        events: (first..=last)
            .map(|seq| event(seq, format!("message {seq}")))
            .collect(),
        next_cursor: last,
        has_more: false,
    }
}

fn detail(version: i64) -> TaskDetail {
    TaskDetail {
        summary: TaskSummary {
            id: 1,
            workspace: "room".to_owned(),
            title: "Review routing".to_owned(),
            state: TaskState::Todo,
            version,
            assigned_agent_id: Some("planner".to_owned()),
            current_attempt_id: None,
            last_executor_id: Some("runner".to_owned()),
            execution_session_id: None,
            last_checkpoint_at: None,
            pause_reason: None,
            stop_evidence: None,
            created_at: 0,
            updated_at: version,
        },
        description: "Review the transport behavior".to_owned(),
        created_by: "operator:admin".to_owned(),
        updated_by: "operator:admin".to_owned(),
        current_attempt: None,
        last_attempt: None,
        checkpoint: None,
        result: None,
        links: Vec::new(),
        external_operations: Vec::new(),
    }
}

fn load_task(state: &mut UiState, detail: TaskDetail) {
    let stamp = state.stamp().unwrap();
    let list = state.tasks.begin_load(stamp.clone());
    assert!(state.apply_task_page(
        &list,
        vec![detail.summary.clone()],
        detail.summary.id,
        false
    ));
    let ticket = state.tasks.begin_detail_load(stamp);
    assert!(state.apply_task_detail(&ticket, detail));
}

fn task_event(seq: i64, task: TaskSummary, change: TaskChange) -> WorkspaceEvent {
    let mut event = event(seq, "");
    event.kind = WorkspaceEventKind::Task;
    event.task_id = Some(task.id);
    event.content = Some(
        serde_json::to_string(&TaskEvent {
            change,
            task,
            attempt_id: None,
            report_id: None,
            external_operation_id: None,
        })
        .unwrap(),
    );
    event
}

fn set_field(form: &mut FormState, id: FieldId, value: &str) {
    let input = &mut form
        .fields
        .iter_mut()
        .find(|field| field.id == id)
        .unwrap()
        .input;
    input.clear();
    input.insert(value).unwrap();
}

fn draw(state: &mut UiState, width: u16, height: u16) -> Buffer {
    state.resize(width, height);
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal.draw(|frame| render(frame, state)).unwrap();
    terminal.backend().buffer().clone()
}

fn screen(buffer: &Buffer) -> String {
    buffer
        .content
        .chunks(usize::from(buffer.area.width))
        .map(|row| {
            let mut text = String::new();
            let mut column = 0;
            while column < row.len() {
                let symbol = row[column].symbol();
                text.push_str(symbol);
                // Wide graphemes cover reset continuation cells, not visible spaces.
                column += UnicodeWidthStr::width(symbol).max(1);
            }
            text
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn grapheme_navigation_and_deletion_preserve_korean_and_combining_clusters() {
    let mut input = InputBuffer::with_text("한e\u{301}글", 64, true).unwrap();
    input.left();
    input.backspace();
    assert_eq!(input.text(), "한글");
    assert_eq!(input.cursor(), "한".len());
    input.insert("e").unwrap();
    input.insert("\u{301}").unwrap();
    input.left();
    input.delete();
    assert_eq!(input.text(), "한글");
    input.home();
    input.right();
    input.delete();
    assert_eq!(input.text(), "한");
    input.insert("\n\u{1112}\u{1161}\u{11ab}e\u{301}").unwrap();
    input.home();
    input.right();
    assert_eq!(
        &input.text()[..input.cursor()],
        "한\n\u{1112}\u{1161}\u{11ab}"
    );
    input.backspace();
    assert_eq!(input.text(), "한\ne\u{301}");
    input.vertical(false);
    assert_eq!(input.cursor(), 0);
    input.vertical(true);
    input.delete();
    assert_eq!(input.text(), "한\n");
}

#[test]
fn paste_limits_are_atomic_and_enter_only_edits_or_changes_focus() {
    let mut input = InputBuffer::with_text("한", 6, true).unwrap();
    input.insert("글").unwrap();
    assert_eq!(input.insert("!"), Err(StateError::InputTooLarge));
    assert_eq!(input.text(), "한글");
    assert_eq!(input.cursor(), 6);
    assert_eq!(
        input.insert(&"x".repeat(100)),
        Err(StateError::InputTooLarge)
    );
    let mut single = InputBuffer::with_text("keep", 64, false).unwrap();
    assert_eq!(single.insert("\r\nsubmit"), Err(StateError::SingleLine));
    assert_eq!(single.text(), "keep");
    let mut form = FormState::new(FormKind::TaskCreate, None).unwrap();
    form.focused_input().unwrap().insert("title").unwrap();
    form.enter().unwrap();
    assert_eq!(form.fields[form.focused].id, FieldId::Description);
    form.focused_input()
        .unwrap()
        .insert("한글\r\ne\u{301}\rq")
        .unwrap();
    form.enter().unwrap();
    assert_eq!(form.value(FieldId::Description), "한글\ne\u{301}\nq\n");
}

#[test]
fn applied_cursor_controls_acknowledgement_and_gap_recovery() {
    let mut state = state();
    state.commit_workspace(workspace("room"));
    let stamp = state.stamp().unwrap();
    let ticket = state.chat.begin_history_load(stamp.clone());
    let applied = state.apply_recent_page(&ticket, page(100, 102)).unwrap();
    assert_eq!(
        applied.iter().map(|effect| effect.ack).collect::<Vec<_>>(),
        vec![Some(100), Some(101), Some(102)]
    );
    assert_eq!(state.chat.applied_cursor, Some(102));
    assert!(state.subscription_live(&stamp, 102));
    let gap = state.apply_event(&stamp, event(104, "not applied yet"));
    assert_eq!(
        gap.disposition,
        EventDisposition::Gap {
            expected: 103,
            received: 104
        }
    );
    assert_eq!(gap.ack, None);
    assert_eq!(state.chat.applied_cursor, Some(102));
    assert_eq!(state.writable(), Err(StateError::Synchronizing));
    assert!(!state.subscription_live(&stamp, 104));
    assert_eq!(
        state.apply_event(&stamp, event(103, "replay")).ack,
        Some(103)
    );
    assert_eq!(
        state.apply_event(&stamp, event(104, "replay")).ack,
        Some(104)
    );
    assert!(state.subscription_live(&stamp, 104));
    let duplicate = state.apply_event(&stamp, event(103, "duplicate"));
    assert_eq!(duplicate.disposition, EventDisposition::Duplicate);
    assert_eq!(
        state
            .chat
            .buffer
            .events()
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        vec![100, 101, 102, 103, 104]
    );
    let mut invalid = event(105, "not valid task JSON");
    invalid.kind = WorkspaceEventKind::Task;
    invalid.task_id = Some(1);
    assert_eq!(state.apply_event(&stamp, invalid).ack, None);
    assert_eq!(state.chat.applied_cursor, Some(104));
    assert!(!state.chat.synchronized);
}

#[test]
fn invalid_history_cannot_seed_ack_cursor_or_replace_visible_history() {
    let mut state = state();
    let stamp = state.stamp().unwrap();
    let ticket = state.chat.begin_history_load(stamp.clone());
    let mut invalid = page(1, 3);
    invalid.events.remove(1);
    assert_eq!(
        state.apply_past_page(&ticket, invalid),
        Err(StateError::InvalidPage)
    );
    assert_eq!(state.chat.mode, HistoryMode::Live);
    assert_eq!(state.chat.applied_cursor, Some(0));
    state.commit_workspace(workspace("room"));
    let ticket = state.chat.begin_history_load(state.stamp().unwrap());
    let mut invalid = page(10, 12);
    invalid.next_cursor = 99;
    assert!(matches!(
        state.apply_recent_page(&ticket, invalid),
        Err(StateError::InvalidPage)
    ));
    assert_eq!(state.chat.applied_cursor, None);
}

#[test]
fn live_cache_enforces_count_and_bytes_and_history_keeps_no_second_body_cache() {
    let mut state = state();
    let stamp = state.stamp().unwrap();
    for seq in 1..=2_100 {
        state.apply_event(&stamp, event(seq, "small"));
    }
    assert_eq!(state.chat.buffer.events().len(), MAX_LIVE_EVENTS);
    assert_eq!(state.chat.buffer.events().front().unwrap().seq, 101);
    assert!(state.chat.buffer.truncated);
    for seq in 2_101..=2_240 {
        state.apply_event(&stamp, event(seq, "x".repeat(64 * 1024)));
    }
    assert!(state.chat.buffer.bytes() <= MAX_EVENT_BYTES);
    assert!(state.chat.buffer.events().front().unwrap().seq > 2_100);
    assert_eq!(state.chat.buffer.events().back().unwrap().seq, 2_240);
    let ticket = state.chat.begin_history_load(stamp.clone());
    state.apply_past_page(&ticket, page(1, 100)).unwrap();
    assert_eq!(state.chat.buffer.events().len(), MAX_HISTORY_EVENTS);
    assert_eq!(state.chat.applied_cursor, Some(2_240));
    let displayed = state.chat.buffer.bytes();
    let effect = state.apply_event(&stamp, event(2_241, "live body not cached in history mode"));
    assert_eq!(effect.ack, Some(2_241));
    assert_eq!(state.chat.buffer.bytes(), displayed);
    assert_eq!(state.chat.buffer.events().back().unwrap().seq, 100);
    assert_eq!(state.chat.new_events, 1);
    state.chat.begin_return_to_live();
    assert!(state.chat.buffer.events().is_empty());
    let ticket = state.chat.begin_history_load(stamp);
    state
        .apply_recent_page(&ticket, page(2_200, 2_241))
        .unwrap();
    assert_eq!(state.chat.applied_cursor, Some(2_241));
    assert_eq!(state.chat.buffer.events().front().unwrap().seq, 2_200);
}

#[test]
fn previous_history_continues_across_short_forward_pages() {
    let mut load = PreviousHistoryLoad::new(201);
    assert_eq!(load.after, 100);
    let mut first = page(101, 120);
    first.has_more = true;
    load.absorb(first).unwrap();
    assert!(!load.done);
    assert_eq!(load.after, 120);
    let mut second = page(121, 200);
    second.has_more = true;
    load.absorb(second).unwrap();
    assert!(load.done);
    assert_eq!(load.buffer.events().len(), 100);
    assert_eq!(load.buffer.events().back().unwrap().seq, 200);
}

#[test]
fn late_workspace_and_same_workspace_revision_responses_are_ignored() {
    let mut state = state();
    let stamp = state.stamp().unwrap();
    let old_list = state.tasks.begin_load(stamp.clone());
    let current_list = state.tasks.begin_load(stamp.clone());
    assert!(!state.apply_task_page(&old_list, vec![detail(1).summary], 1, false));
    assert!(state.apply_task_page(&current_list, vec![detail(2).summary], 1, false));
    let old_history = state.chat.begin_history_load(stamp.clone());
    let current_history = state.chat.begin_history_load(stamp.clone());
    assert!(
        state
            .apply_recent_page(&old_history, page(1, 2))
            .unwrap()
            .is_empty()
    );
    assert!(state.chat.buffer.events().is_empty());
    state
        .apply_recent_page(&current_history, page(1, 2))
        .unwrap();
    let old_members = state.begin_members_load().unwrap();
    state.commit_workspace(workspace("other"));
    state.commit_workspace(workspace("room"));
    assert!(!state.apply_members(&old_members, Vec::new()));
    assert!(!state.apply_task_page(&current_list, vec![detail(5).summary], 1, false));
    assert_eq!(
        state
            .apply_event(&stamp, event(3, "old connection"))
            .disposition,
        EventDisposition::StaleWorkspace
    );
    assert_eq!(state.chat.applied_cursor, None);
}

#[test]
fn task_events_win_over_list_detail_and_history_refresh_snapshots() {
    let mut state = state();
    load_task(&mut state, detail(1));
    let stamp = state.stamp().unwrap();
    let list = state.tasks.begin_load(stamp.clone());
    let old_detail = state.tasks.begin_detail_load(stamp.clone());
    let mut latest = detail(3);
    latest.summary.state = TaskState::Paused;
    latest.summary.stop_evidence = Some(StopEvidence::Unknown);
    let effect = state.apply_event(
        &stamp,
        task_event(1, latest.summary.clone(), TaskChange::Interrupted),
    );
    assert!(effect.refresh_task_detail);
    assert!(!state.tasks.can_request());
    assert!(state.apply_task_page(&list, vec![detail(2).summary], 1, false));
    assert_eq!(state.tasks.rows[&1].version, 3);
    assert!(!state.apply_task_detail(&old_detail, detail(2)));
    assert!(state.tasks.detail_stale);
    let ticket = state.tasks.begin_detail_load(stamp.clone());
    assert!(state.apply_task_detail(&ticket, latest));
    assert!(!state.tasks.can_request());
    let history = state.chat.begin_history_load(stamp);
    let mut old_page = page(1, 1);
    old_page.events = vec![task_event(1, detail(1).summary, TaskChange::Created)];
    state.apply_recent_page(&history, old_page).unwrap();
    assert_eq!(state.tasks.selected_summary.as_ref().unwrap().version, 3);
}

#[test]
fn version_reconfirmation_preserves_draft_and_allocates_a_new_operation() {
    let state = state();
    let original = detail(1);
    let mut form = FormState::new(FormKind::TaskEdit, Some(&original)).unwrap();
    set_field(&mut form, FieldId::Title, "내 초안");
    assert!(matches!(
        form.prepare(state.stamp().unwrap()),
        Err(StateError::ReconfirmationRequired)
    ));
    form.check_latest(&original).unwrap();
    let prepared = form.prepare(state.stamp().unwrap()).unwrap();
    let latest = detail(2);
    assert_eq!(
        form.check_latest(&latest),
        Err(StateError::ReconfirmationRequired)
    );
    assert_eq!(form.value(FieldId::Title), "내 초안");
    assert_eq!(form.baseline.as_ref().unwrap().version, 1);
    assert!(matches!(
        form.prepare(state.stamp().unwrap()),
        Err(StateError::ReconfirmationRequired)
    ));
    form.reconfirm_latest().unwrap();
    assert!(matches!(
        form.prepare(state.stamp().unwrap()),
        Err(StateError::ReconfirmationRequired)
    ));
    form.check_latest(&latest).unwrap();
    let mut fresh = form.prepare(state.stamp().unwrap()).unwrap();
    assert_ne!(fresh.operation_id(), prepared.operation_id());
    match fresh.submit().unwrap() {
        ClientMessage::TaskEdit {
            expected_version,
            title,
            ..
        } => {
            assert_eq!(expected_version, 2);
            assert_eq!(title.as_deref(), Some("내 초안"));
        }
        _ => panic!("unexpected mutation variant"),
    }
    let wrong_room = WorkspaceStamp {
        generation: 9,
        workspace: workspace("other"),
    };
    assert!(matches!(
        form.prepare(wrong_room),
        Err(StateError::MissingTask)
    ));
}

#[test]
fn uncertain_retry_keeps_payload_and_receipts_do_not_rewind_authoritative_state() {
    let mut state = state();
    load_task(&mut state, detail(1));
    let mut form = FormState::new(FormKind::TaskEdit, Some(&detail(1))).unwrap();
    set_field(&mut form, FieldId::Description, "immutable description");
    form.check_latest(&detail(1)).unwrap();
    let mut mutation = state.prepare_form(&form).unwrap();
    let sent = mutation.submit().unwrap();
    assert!(matches!(mutation.retry(), Err(StateError::NotUncertain)));
    mutation.mark_uncertain();
    set_field(&mut form, FieldId::Description, "later draft");
    assert_eq!(
        serde_json::to_value(mutation.retry().unwrap()).unwrap(),
        serde_json::to_value(sent).unwrap()
    );
    let receipt = TaskMutationResult {
        operation_id: mutation.operation_id().unwrap(),
        applied_version: 2,
        report_id: None,
        task: detail(2),
    };
    state.mutation = Some(mutation);
    let stamp = state.stamp().unwrap();
    state.apply_event(&stamp, task_event(1, detail(3).summary, TaskChange::Edited));
    let mut wrong = receipt.clone();
    wrong.operation_id = Uuid::from_u128(999);
    assert_eq!(
        state.apply_receipt(&stamp, wrong),
        Err(StateError::WrongReceipt)
    );
    assert!(state.apply_receipt(&stamp, receipt).unwrap());
    assert_eq!(
        state.mutation.as_ref().unwrap().stage,
        MutationStage::Applied
    );
    assert_eq!(state.tasks.selected_summary.as_ref().unwrap().version, 3);
    assert!(state.tasks.detail_stale);
}

#[test]
fn request_lifetime_and_unknown_stop_evidence_are_not_task_completion() {
    let mut state = state();
    load_task(&mut state, detail(1));
    let mut form = FormState::new(FormKind::TaskRequest, Some(&detail(1))).unwrap();
    form.check_latest(&detail(1)).unwrap();
    let mut request = state.prepare_form(&form).unwrap();
    let request_id = request.request_id().to_owned();
    state.track_request(&request).unwrap();
    request.submit().unwrap();
    request.mark_uncertain();
    assert!(matches!(
        request.retry(),
        Err(StateError::RequestNotRetryable)
    ));
    state.request_accepted(&request_id);
    assert_eq!(
        state.pending_requests[&request_id].stage,
        RequestStage::Accepted
    );
    assert_eq!(
        state.tasks.selected_summary.as_ref().unwrap().state,
        TaskState::Todo
    );
    assert!(state.detach_needs_confirmation());
    assert_eq!(
        state.begin_switch(workspace("other")),
        Err(StateError::Busy)
    );
    let stamp = state.stamp().unwrap();
    let mut begun = detail(2).summary;
    begun.state = TaskState::InProgress;
    begun.current_attempt_id = Some(Uuid::from_u128(2));
    state.apply_event(&stamp, task_event(1, begun, TaskChange::Begun));
    assert_eq!(
        state.pending_requests[&request_id].stage,
        RequestStage::ExecutionObserved
    );
    let mut result = event(2, "request transport result");
    result.kind = WorkspaceEventKind::Result;
    result.request_id = Some(request_id);
    result.ok = Some(true);
    assert!(state.apply_event(&stamp, result).request_resolved);
    assert!(!state.detach_needs_confirmation());
    assert_eq!(
        state.tasks.selected_summary.as_ref().unwrap().state,
        TaskState::InProgress
    );
    let mut interrupted = detail(3);
    interrupted.summary.state = TaskState::Paused;
    interrupted.summary.stop_evidence = Some(StopEvidence::Unknown);
    let attempt_id = Uuid::from_u128(2);
    interrupted.last_attempt = Some(TaskAttempt {
        id: attempt_id,
        task_id: 1,
        agent_id: "runner".to_owned(),
        session_id: Uuid::from_u128(3),
        work_request_id: "work".to_owned(),
        resumed_from_checkpoint_id: None,
        status: AttemptStatus::Interrupted,
        stop_evidence: StopEvidence::Unknown,
        reason: None,
        started_at: 0,
        ended_at: Some(1),
        stopped_at: None,
    });
    load_task(&mut state, interrupted.clone());
    assert!(!state.tasks.can_request());
    let mut blocked = FormState::new(FormKind::TaskRequest, Some(&interrupted)).unwrap();
    assert_eq!(
        blocked.check_latest(&interrupted),
        Err(StateError::StopUnconfirmed)
    );
    let mut confirm = FormState::new(FormKind::TaskConfirmStopped, Some(&interrupted)).unwrap();
    set_field(&mut confirm, FieldId::Note, "Observed the process exit");
    confirm.check_latest(&interrupted).unwrap();
    assert!(matches!(
        state.prepare_form(&confirm),
        Err(StateError::MustObserveStopped)
    ));
    confirm.focused = confirm.fields.len();
    confirm.toggle_checkbox();
    let mut mutation = state.prepare_form(&confirm).unwrap();
    assert!(
        matches!(mutation.submit().unwrap(), ClientMessage::TaskConfirmStopped { attempt_id: id, expected_version: 3, .. } if id == attempt_id)
    );
}

#[test]
fn chat_draft_is_retained_until_matching_durable_ack_and_retry_is_immutable() {
    let mut state = state();
    state.composer.input.insert("한글 message").unwrap();
    let sent = state.composer.prepare().unwrap();
    let ClientMessage::WorkspacePost { request_id, .. } = &sent else {
        panic!("expected chat post")
    };
    state.composer.mark_uncertain();
    assert_eq!(
        serde_json::to_value(state.composer.retry().unwrap()).unwrap(),
        serde_json::to_value(&sent).unwrap()
    );
    let stamp = state.stamp().unwrap();
    let mut unrelated = event(1, "different text");
    unrelated.request_id = Some(request_id.clone());
    state.apply_event(&stamp, unrelated);
    assert!(state.composer.is_locked());
    assert_eq!(state.composer.input.text(), "한글 message");
    let mut matching = event(2, "한글 message");
    matching.request_id = Some(request_id.clone());
    state.apply_event(&stamp, matching);
    assert!(!state.composer.is_locked());
    assert_eq!(state.composer.input.text(), "");
}

#[test]
fn reconnect_invalidates_reads_and_fences_writes_until_replay_is_applied() {
    let mut state = state();
    let stamp = state.stamp().unwrap();
    let ticket = state.tasks.begin_load(stamp.clone());
    state.connection_changed(Connection::Reconnecting);
    assert_eq!(state.writable(), Err(StateError::NotConnected));
    assert!(!state.apply_task_page(&ticket, vec![detail(1).summary], 1, false));
    assert!(state.connection_changed(Connection::Connected { epoch: 2 }));
    assert_eq!(state.writable(), Err(StateError::Synchronizing));
    state.apply_event(&stamp, event(1, "replay"));
    assert!(state.subscription_live(&stamp, 1));
    assert_eq!(state.writable(), Ok(()));
}

#[test]
fn responsive_layout_retains_separate_assignee_executor_and_small_terminal_write_fence() {
    let mut state = state();
    state.workspaces.rows.push(WorkspaceSummary {
        name: workspace("room"),
        created_at: 0,
        connected_agents: 2,
    });
    load_task(&mut state, detail(1));
    state.tab = Tab::Tasks;
    let narrow = draw(&mut state, 80, 24);
    let text = screen(&narrow);
    assert!(text.contains("Assignee: planner"));
    assert!(text.contains("Executor: runner"));
    assert!(!text.contains("Detail ·"));
    assert_eq!(state.writable(), Ok(()));
    let wide = draw(&mut state, 120, 35);
    assert!(screen(&wide).contains("Detail ·"));
    assert_eq!(wide[(86, 3)].symbol(), "┌");
    state.modal = Some(Modal::Detail {
        kind: DetailKind::Task,
        scroll: 0,
    });
    assert!(screen(&draw(&mut state, 80, 24)).contains("Task #1"));
    state.modal = None;
    let small = draw(&mut state, 79, 23);
    assert_eq!(state.writable(), Err(StateError::TerminalTooSmall));
    assert!(!screen(&small).contains("Assignee: planner"));
    assert_eq!(state.connection, Connection::Connected { epoch: 1 });
}

#[test]
fn untrusted_controls_are_inert_in_test_backend_and_no_color_keeps_labels() {
    let mut state = state();
    state.render.no_color = true;
    state.header.profile = Some("office\u{001B}]52;c;hidden\u{0007}".to_owned());
    let stamp = state.stamp().unwrap();
    state.apply_event(
        &stamp,
        event(
            1,
            "\u{001B}[31mRED\u{009B}2J\u{001B}]52;c;payload\u{0007}\n한글 e\u{301}",
        ),
    );
    state.modal = Some(Modal::Detail {
        kind: DetailKind::Event(1),
        scroll: 0,
    });
    let buffer = draw(&mut state, 120, 35);
    for cell in &buffer.content {
        assert!(
            !cell
                .symbol()
                .chars()
                .any(|ch| ch <= '\u{001F}' || ('\u{007F}'..='\u{009F}').contains(&ch))
        );
        assert_eq!(cell.fg, Color::Reset);
        assert_eq!(cell.bg, Color::Reset);
    }
    let text = screen(&buffer);
    assert!(text.contains("\\u{001B}[31mRED"));
    assert!(text.contains("\\u{009B}2J"));
    assert!(text.contains("\\u{0007}"));
    assert!(text.contains("한글 e\u{301}"));
    assert!(text.contains("CHAT"));
    assert_eq!(
        display_endpoint("wss://user:secret@example.com/private?token=hidden#hidden"),
        "wss://example.com/ws"
    );
}

fn key(state: &mut UiState, code: KeyCode) -> UiInput {
    handle(state, Event::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

fn ctrl(state: &mut UiState, value: char) -> UiInput {
    handle(
        state,
        Event::Key(KeyEvent::new(KeyCode::Char(value), KeyModifiers::CONTROL)),
    )
}

#[test]
fn keyboard_text_focus_preserves_unicode_aliases_and_atomic_paste() {
    let mut state = state();
    key(&mut state, KeyCode::Char('i'));
    for letter in "qari?".chars() {
        assert!(matches!(
            key(&mut state, KeyCode::Char(letter)),
            UiInput::None
        ));
    }
    assert_eq!(state.composer.input.text(), "qari?");
    assert!(state.modal.is_none());
    ctrl(&mut state, 'j');
    handle(&mut state, Event::Paste("한글 e\u{301}👩‍💻".to_owned()));
    key(&mut state, KeyCode::Left);
    key(&mut state, KeyCode::Backspace);
    assert_eq!(state.composer.input.text(), "qari?\n한글 👩‍💻");
    let text = state.composer.input.text().to_owned();
    handle(&mut state, Event::Paste("x".repeat(64 * 1024)));
    assert_eq!(state.composer.input.text(), text);
    assert!(!state.composer.is_locked());
    assert!(matches!(
        ctrl(&mut state, 's'),
        UiInput::Command(UiCommand::SubmitChat)
    ));
    handle(
        &mut state,
        Event::Paste("cannot race queued submit".to_owned()),
    );
    assert_eq!(state.composer.input.text(), text);
    command_not_sent(&mut state, &UiCommand::SubmitChat);
    assert!(!state.form_busy);
    ctrl(&mut state, 'i');
    assert_eq!(state.focus, Focus::Detail);
    let mut release = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
    release.kind = KeyEventKind::Release;
    assert!(matches!(
        handle(&mut state, Event::Key(release)),
        UiInput::None
    ));
}

#[test]
fn keyboard_task_actions_require_real_detail_and_never_submit_on_enter() {
    let mut state = state();
    assert!(matches!(
        key(&mut state, KeyCode::F(2)),
        UiInput::Command(UiCommand::LoadTasks)
    ));
    key(&mut state, KeyCode::Char('e'));
    assert!(state.form().is_none());
    load_task(&mut state, detail(1));
    for (action, expected) in [
        ('a', FormKind::TaskAssign),
        ('r', FormKind::TaskRequest),
        ('i', FormKind::TaskInterrupt),
        ('e', FormKind::TaskEdit),
        ('m', FormKind::TaskNote),
        ('c', FormKind::TaskCancel),
        ('o', FormKind::TaskReopen),
    ] {
        key(&mut state, KeyCode::Char(action));
        assert_eq!(state.form().unwrap().kind, expected);
        assert_eq!(state.form().unwrap().baseline.as_ref().unwrap().version, 1);
        assert!(matches!(key(&mut state, KeyCode::Enter), UiInput::None));
        assert!(state.mutation.is_none());
        key(&mut state, KeyCode::Esc);
    }
    ctrl(&mut state, 't');
    handle(&mut state, Event::Paste("new task".to_owned()));
    key(&mut state, KeyCode::Enter);
    handle(&mut state, Event::Paste("line one\r\nline two".to_owned()));
    assert_eq!(
        state.form().unwrap().value(FieldId::Description),
        "line one\nline two"
    );
    assert!(state.form().unwrap().baseline.is_none());
    assert!(matches!(
        ctrl(&mut state, 's'),
        UiInput::Command(UiCommand::SubmitForm)
    ));
    key(&mut state, KeyCode::Esc);
    key(&mut state, KeyCode::Char('q'));
    handle(
        &mut state,
        Event::Paste("mutated after preflight".to_owned()),
    );
    assert!(state.form_busy);
    assert_eq!(
        state.form().unwrap().value(FieldId::Description),
        "line one\nline two"
    );
}

#[test]
fn keyboard_conflict_reconfirmation_and_uncertain_retry_keep_distinct_operations() {
    let mut state = state();
    load_task(&mut state, detail(1));
    key(&mut state, KeyCode::F(2));
    key(&mut state, KeyCode::Char('e'));
    set_field(state.form_mut().unwrap(), FieldId::Title, "preserved draft");
    state.form_mut().unwrap().check_latest(&detail(1)).unwrap();
    let mut old = state.prepare_form(state.form().unwrap()).unwrap();
    old.submit().unwrap();
    let old_id = old.operation_id();
    old.conflict(Some(2));
    state.mutation = Some(old);
    assert_eq!(
        state.form_mut().unwrap().check_latest(&detail(2)),
        Err(StateError::ReconfirmationRequired)
    );
    assert!(matches!(ctrl(&mut state, 's'), UiInput::None));
    assert!(!state.form_busy);
    assert!(matches!(ctrl(&mut state, 'r'), UiInput::None));
    assert!(state.mutation.is_none());
    assert_eq!(state.form().unwrap().baseline.as_ref().unwrap().version, 2);
    assert_eq!(
        state.form().unwrap().value(FieldId::Title),
        "preserved draft"
    );
    assert!(matches!(
        state.prepare_form(state.form().unwrap()),
        Err(StateError::ReconfirmationRequired)
    ));
    state.form_mut().unwrap().check_latest(&detail(2)).unwrap();
    let mut fresh = state.prepare_form(state.form().unwrap()).unwrap();
    assert_ne!(fresh.operation_id(), old_id);
    let sent = fresh.submit().unwrap();
    fresh.mark_uncertain();
    state.mutation = Some(fresh);
    handle(&mut state, Event::Paste("changed payload".to_owned()));
    ctrl(&mut state, 'r');
    assert_eq!(
        state.form().unwrap().value(FieldId::Title),
        "preserved draft"
    );
    assert!(matches!(
        ctrl(&mut state, 's'),
        UiInput::Command(UiCommand::RetryMutation)
    ));
    assert_eq!(
        serde_json::to_value(state.mutation.as_mut().unwrap().retry().unwrap()).unwrap(),
        serde_json::to_value(sent).unwrap(),
    );
}

#[test]
fn detach_overlay_preserves_frozen_form_and_requires_literal_confirmation() {
    let mut state = state();
    load_task(&mut state, detail(1));
    let mut request = FormState::new(FormKind::TaskRequest, Some(&detail(1))).unwrap();
    request.check_latest(&detail(1)).unwrap();
    let request = state.prepare_form(&request).unwrap();
    state.track_request(&request).unwrap();
    ctrl(&mut state, 't');
    handle(&mut state, Event::Paste("survives overlay".to_owned()));
    ctrl(&mut state, 's');
    assert!(matches!(request_detach(&mut state), UiInput::None));
    assert!(!state.detaching);
    assert!(matches!(state.modal, Some(Modal::Detach { .. })));
    assert_eq!(
        state.form().unwrap().value(FieldId::Title),
        "survives overlay"
    );
    handle(&mut state, Event::Paste("DETACH\n".to_owned()));
    assert!(!state.detach_confirmed());
    assert!(matches!(ctrl(&mut state, 's'), UiInput::None));
    key(&mut state, KeyCode::Esc);
    assert!(matches!(state.modal, Some(Modal::Form(_))));
    assert!(state.form_busy);
    assert!(!state.detaching);
    request_detach(&mut state);
    state.close_form();
    assert!(matches!(state.modal, Some(Modal::Detach { .. })));
    assert!(state.form().is_none());
    assert!(!state.form_busy);
    handle(&mut state, Event::Paste("DETACH".to_owned()));
    assert!(matches!(ctrl(&mut state, 's'), UiInput::Detach));
    assert!(state.detaching);
}

#[test]
fn keyboard_write_fences_and_stop_permission_survive_paste() {
    let mut state = state();
    state.header.ownership = Ownership::Remote;
    ctrl(&mut state, 'x');
    assert!(state.modal.is_none());
    key(&mut state, KeyCode::F(4));
    assert!(state.form().is_none());
    state.header.ownership = Ownership::Owned;
    state.header.is_admin = false;
    ctrl(&mut state, 'n');
    assert!(state.form().is_none());
    state.header.is_admin = true;
    ctrl(&mut state, 'x');
    handle(&mut state, Event::Paste("STOP".to_owned()));
    key(&mut state, KeyCode::Enter);
    assert!(!state.form_busy);
    state.resize(79, 24);
    assert!(matches!(ctrl(&mut state, 's'), UiInput::None));
    state.resize(80, 24);
    state.connection_changed(Connection::Reconnecting);
    assert!(matches!(ctrl(&mut state, 's'), UiInput::None));
    state.connection_changed(Connection::Connected { epoch: 2 });
    assert!(matches!(ctrl(&mut state, 's'), UiInput::None));
    let stamp = state.stamp().unwrap();
    assert!(state.subscription_live(&stamp, 0));
    assert!(matches!(
        ctrl(&mut state, 's'),
        UiInput::Command(UiCommand::ConfirmStop)
    ));
    command_not_sent(&mut state, &UiCommand::ConfirmStop);
    key(&mut state, KeyCode::Esc);
    assert!(matches!(ctrl(&mut state, 'c'), UiInput::Detach));
}

#[test]
fn keyboard_filters_pagination_history_and_invite_actions_are_contextual() {
    let mut state = state();
    load_task(&mut state, detail(1));
    key(&mut state, KeyCode::F(2));
    state.tasks.pagination.next_cursor = Some(10);
    state.tasks.pagination.has_more = true;
    assert!(matches!(
        key(&mut state, KeyCode::Char(']')),
        UiInput::Command(UiCommand::LoadTasks)
    ));
    key(&mut state, KeyCode::Char('/'));
    set_field(state.form_mut().unwrap(), FieldId::States, "paused,done");
    set_field(state.form_mut().unwrap(), FieldId::Agent, "offline-agent");
    assert!(matches!(
        ctrl(&mut state, 's'),
        UiInput::Command(UiCommand::LoadTasks)
    ));
    assert_eq!(
        state.tasks.filter.states,
        vec![TaskState::Paused, TaskState::Done]
    );
    assert_eq!(
        state.tasks.filter.assigned_agent_id.as_deref(),
        Some("offline-agent")
    );
    assert_eq!(state.tasks.pagination.after, None);
    assert!(state.form().is_none());
    state.tasks.set_filter(TaskFilter::default());
    load_task(&mut state, detail(1));
    assert!(matches!(
        key(&mut state, KeyCode::Char('h')),
        UiInput::Command(UiCommand::LoadTaskHistory)
    ));
    state.tasks.history_pagination.next_cursor = Some(20);
    state.tasks.history_pagination.has_more = true;
    assert!(matches!(
        key(&mut state, KeyCode::Char(']')),
        UiInput::Command(UiCommand::LoadTaskHistory)
    ));
    key(&mut state, KeyCode::PageDown);
    assert!(matches!(state.modal, Some(Modal::Detail { scroll, .. }) if scroll > 0));
    key(&mut state, KeyCode::Esc);
    key(&mut state, KeyCode::F(4));
    assert_eq!(state.form().unwrap().kind, FormKind::Invite);
    assert!(matches!(key(&mut state, KeyCode::Char('p')), UiInput::None));
    assert_eq!(state.form().unwrap().value(FieldId::Profile), "p");
    key(&mut state, KeyCode::Esc);
    state.modal = Some(Modal::InviteReady {
        workspace: workspace("room"),
        provider: None,
        expires_at: 0,
    });
    assert!(matches!(
        key(&mut state, KeyCode::Char('p')),
        UiInput::PrintPrompt
    ));
    assert!(matches!(
        key(&mut state, KeyCode::Char('y')),
        UiInput::CopyPrompt
    ));
    assert!(matches!(
        key(&mut state, KeyCode::Esc),
        UiInput::DiscardPrompt
    ));
    assert!(state.modal.is_none());
}

#[test]
fn previous_history_completion_cannot_cross_revisions_modes_or_workspaces() {
    let mut state = state();
    let stamp = state.stamp().unwrap();
    let recent = state.chat.begin_history_load(stamp.clone());
    state.apply_recent_page(&recent, page(201, 210)).unwrap();
    state.chat.enter_past();
    let ticket = state.chat.begin_history_load(stamp.clone());
    let mut load = PreviousHistoryLoad::new(201);
    load.absorb(page(101, 200)).unwrap();
    assert!(state.apply_previous_page(&ticket, load.clone()).unwrap());
    assert_eq!(state.chat.buffer.events().back().unwrap().seq, 200);
    assert_eq!(state.chat.applied_cursor, Some(0));
    state.chat.begin_history_load(stamp.clone());
    assert!(!state.apply_previous_page(&ticket, load.clone()).unwrap());
    let ticket = state.chat.begin_history_load(stamp);
    state.chat.begin_return_to_live();
    assert!(!state.apply_previous_page(&ticket, load.clone()).unwrap());
    state.chat.enter_past();
    let ticket = state.chat.begin_history_load(state.stamp().unwrap());
    state.commit_workspace(workspace("other"));
    assert!(!state.apply_previous_page(&ticket, load).unwrap());
    assert!(state.chat.buffer.events().is_empty());
}

#[test]
fn previous_history_boundary_inside_full_forward_page_preserves_absolute_cursors() {
    let mut state = state();
    state.commit_workspace(workspace("room"));
    let stamp = state.stamp().unwrap();
    let recent = state.chat.begin_history_load(stamp.clone());
    state.apply_recent_page(&recent, page(54, 153)).unwrap();
    assert!(state.subscription_live(&stamp, 153));
    assert!(matches!(
        key(&mut state, KeyCode::Char('[')),
        UiInput::Command(UiCommand::PreviousHistory)
    ));
    let mut load = PreviousHistoryLoad::new(state.chat.buffer.events().front().unwrap().seq);
    assert_eq!(load.after, 0);
    state.chat.enter_past();
    let ticket = state.chat.begin_history_load(stamp.clone());
    let mut forward = page(1, 100);
    forward.has_more = true;
    load.absorb(forward).unwrap();
    assert!(load.done);
    assert_eq!(load.after, 53);
    assert!(state.apply_previous_page(&ticket, load).unwrap());
    assert_eq!(state.chat.buffer.events().front().unwrap().seq, 1);
    assert_eq!(state.chat.buffer.events().back().unwrap().seq, 53);
    assert_eq!(state.chat.selected_seq, Some(53));
    assert_eq!(state.chat.display.next_cursor, 53);
    assert!(state.chat.display.has_more);
    assert_eq!(state.chat.applied_cursor, Some(153));
    assert_eq!(state.writable(), Ok(()));
    assert!(matches!(
        key(&mut state, KeyCode::Char(']')),
        UiInput::Command(UiCommand::NextHistory)
    ));
    let next = state.chat.begin_history_load(stamp.clone());
    assert!(state.apply_past_page(&next, page(54, 153)).unwrap());
    assert!(!state.chat.display.has_more);
    let effect = state.apply_event(&stamp, event(154, "new live message"));
    assert_eq!(effect.ack, Some(154));
    assert_eq!(state.chat.applied_cursor, Some(154));
    assert_eq!(state.chat.buffer.events().back().unwrap().seq, 153);
    assert_eq!(state.chat.display.next_cursor, 153);
    assert!(state.chat.display.has_more);
    assert!(matches!(
        key(&mut state, KeyCode::Char(']')),
        UiInput::Command(UiCommand::NextHistory)
    ));
    // A response captured before the live event cannot hide that known successor.
    let late = state.chat.begin_history_load(stamp);
    assert!(state.apply_past_page(&late, page(54, 153)).unwrap());
    assert!(state.chat.display.has_more);
    assert_eq!(state.writable(), Ok(()));
}

#[test]
fn rejected_display_history_keeps_live_authority_and_current_page() {
    let mut state = state();
    state.commit_workspace(workspace("room"));
    let stamp = state.stamp().unwrap();
    let recent = state.chat.begin_history_load(stamp.clone());
    state.apply_recent_page(&recent, page(54, 153)).unwrap();
    assert!(state.subscription_live(&stamp, 153));
    let past = state.chat.begin_history_load(stamp.clone());
    assert!(state.apply_past_page(&past, page(1, 53)).unwrap());
    let ticket = state.chat.begin_history_load(stamp.clone());
    let mut invalid = page(54, 153);
    invalid.next_cursor = 100;
    assert_eq!(
        state.apply_past_page(&ticket, invalid),
        Err(StateError::InvalidPage)
    );
    let unfinished = PreviousHistoryLoad::new(54);
    assert_eq!(
        state.apply_previous_page(&ticket, unfinished),
        Err(StateError::InvalidPage)
    );
    assert_eq!(state.chat.mode, HistoryMode::Past);
    assert_eq!(state.chat.buffer.events().front().unwrap().seq, 1);
    assert_eq!(state.chat.buffer.events().back().unwrap().seq, 53);
    assert_eq!(state.chat.display.next_cursor, 53);
    assert_eq!(state.chat.applied_cursor, Some(153));
    assert_eq!(state.writable(), Ok(()));
    assert_eq!(
        state.apply_event(&stamp, event(154, "still live")).ack,
        Some(154)
    );
    assert_eq!(state.chat.buffer.events().back().unwrap().seq, 53);
    assert_eq!(state.writable(), Ok(()));
}

#[test]
fn keyboard_does_not_retry_requests_or_workspace_creation() {
    let mut state = state();
    load_task(&mut state, detail(1));
    let mut form = FormState::new(FormKind::TaskRequest, Some(&detail(1))).unwrap();
    form.check_latest(&detail(1)).unwrap();
    let mut mutation = state.prepare_form(&form).unwrap();
    mutation.submit().unwrap();
    mutation.mark_uncertain();
    state.mutation = Some(mutation);
    state.modal = Some(Modal::Form(form));
    assert!(matches!(ctrl(&mut state, 's'), UiInput::None));
    assert_eq!(
        state.form().unwrap().error,
        Some(StateError::RequestNotRetryable)
    );
    state.close_form();
    state.mutation = None;
    ctrl(&mut state, 'n');
    handle(&mut state, Event::Paste("new-room".to_owned()));
    let mut mutation = state.prepare_form(state.form().unwrap()).unwrap();
    mutation.submit().unwrap();
    mutation.mark_uncertain();
    state.mutation = Some(mutation);
    assert!(matches!(ctrl(&mut state, 's'), UiInput::None));
    assert_eq!(
        state.form().unwrap().error,
        Some(StateError::RequestNotRetryable)
    );
}

#[test]
fn keyboard_assignee_picker_supports_members_offline_ids_and_unassign() {
    use agent_session_router::protocol::{
        AgentClient, AgentDescriptor, AgentSide, AgentStatus, DeliveryMode,
    };
    let mut state = state();
    load_task(&mut state, detail(1));
    state.members = vec![AgentDescriptor {
        agent_id: "connected-agent".to_owned(),
        side: AgentSide::Generic,
        client: AgentClient::Generic,
        activity: None,
        status: AgentStatus::Idle,
        delivery_mode: DeliveryMode::Pull,
        ready: true,
        session_id: Uuid::from_u128(20),
    }];
    key(&mut state, KeyCode::F(2));
    assert!(matches!(
        key(&mut state, KeyCode::Char('a')),
        UiInput::Command(UiCommand::LoadMembers)
    ));
    key(&mut state, KeyCode::Down);
    assert_eq!(
        state.form().unwrap().value(FieldId::Agent),
        "connected-agent"
    );
    key(&mut state, KeyCode::Down);
    assert_eq!(state.form().unwrap().value(FieldId::Agent), "");
    handle(&mut state, Event::Paste("offline-agent".to_owned()));
    state.form_mut().unwrap().check_latest(&detail(1)).unwrap();
    let mut mutation = state.prepare_form(state.form().unwrap()).unwrap();
    assert!(
        matches!(mutation.submit().unwrap(), ClientMessage::TaskAssign { agent_id: Some(id), .. } if id == "offline-agent")
    );
    assert!(state.pending_requests.is_empty());
    assert!(state.mutation.is_none());
    key(&mut state, KeyCode::Esc);
    assert!(matches!(
        key(&mut state, KeyCode::F(3)),
        UiInput::Command(UiCommand::LoadMembers)
    ));
    key(&mut state, KeyCode::Enter);
    assert!(matches!(
        state.modal,
        Some(Modal::Detail {
            kind: DetailKind::Member(0),
            ..
        })
    ));
    key(&mut state, KeyCode::PageDown);
    assert!(matches!(state.modal, Some(Modal::Detail { scroll, .. }) if scroll > 0));
    key(&mut state, KeyCode::Esc);
    key(&mut state, KeyCode::BackTab);
    assert_eq!(state.focus, Focus::Workspaces);
    let revision = state.workspaces.begin_load();
    state.workspaces.apply(
        revision,
        vec![WorkspaceSummary {
            name: workspace("other"),
            created_at: 0,
            connected_agents: 0,
        }],
        Some("other".to_owned()),
        true,
    );
    assert!(
        matches!(key(&mut state, KeyCode::Enter), UiInput::Command(UiCommand::SelectWorkspace(name)) if name == workspace("other"))
    );
    assert!(matches!(
        key(&mut state, KeyCode::Char(']')),
        UiInput::Command(UiCommand::LoadWorkspaces)
    ));
    assert!(matches!(
        key(&mut state, KeyCode::Char('[')),
        UiInput::Command(UiCommand::LoadWorkspaces)
    ));
}

#[test]
fn keyboard_confirm_stopped_requires_observation_and_disables_confirmed_evidence() {
    let mut state = state();
    let mut interrupted = detail(3);
    interrupted.summary.state = TaskState::Paused;
    interrupted.summary.stop_evidence = Some(StopEvidence::Unknown);
    let attempt_id = Uuid::from_u128(2);
    interrupted.last_attempt = Some(TaskAttempt {
        id: attempt_id,
        task_id: 1,
        agent_id: "runner".to_owned(),
        session_id: Uuid::from_u128(3),
        work_request_id: "work".to_owned(),
        resumed_from_checkpoint_id: None,
        status: AttemptStatus::Interrupted,
        stop_evidence: StopEvidence::Unknown,
        reason: None,
        started_at: 0,
        ended_at: Some(1),
        stopped_at: None,
    });
    load_task(&mut state, interrupted.clone());
    key(&mut state, KeyCode::F(2));
    key(&mut state, KeyCode::Char('f'));
    handle(&mut state, Event::Paste("Observed process exit".to_owned()));
    state
        .form_mut()
        .unwrap()
        .check_latest(&interrupted)
        .unwrap();
    assert!(matches!(
        state.prepare_form(state.form().unwrap()),
        Err(StateError::MustObserveStopped)
    ));
    key(&mut state, KeyCode::Tab);
    handle(&mut state, Event::Paste(" ".to_owned()));
    assert!(!state.form().unwrap().stopped_observed);
    key(&mut state, KeyCode::Char(' '));
    let mut mutation = state.prepare_form(state.form().unwrap()).unwrap();
    assert!(
        matches!(mutation.submit().unwrap(), ClientMessage::TaskConfirmStopped { attempt_id: exact, expected_version: 3, .. } if exact == attempt_id)
    );
    key(&mut state, KeyCode::Esc);
    interrupted.summary.version = 4;
    interrupted.summary.stop_evidence = Some(StopEvidence::Confirmed);
    interrupted.last_attempt.as_mut().unwrap().stop_evidence = StopEvidence::Confirmed;
    load_task(&mut state, interrupted);
    key(&mut state, KeyCode::Char('f'));
    assert!(state.form().is_none());
    assert!(state.tasks.confirmable_attempt().is_none());
}

#[test]
fn accepted_detach_fences_queued_request_before_tracking_or_dispatch() {
    let mut state = state();
    load_task(&mut state, detail(1));
    key(&mut state, KeyCode::F(2));
    key(&mut state, KeyCode::Char('r'));
    assert!(matches!(
        ctrl(&mut state, 's'),
        UiInput::Command(UiCommand::SubmitForm)
    ));
    assert!(state.pending_requests.is_empty());
    assert!(matches!(ctrl(&mut state, 'c'), UiInput::Detach));
    assert!(state.detaching);
    // A latest-detail preflight may finish after input accepted the exit.
    state.form_mut().unwrap().check_latest(&detail(1)).unwrap();
    assert!(matches!(
        state.prepare_form(state.form().unwrap()),
        Err(StateError::Busy)
    ));
    let prepared = state
        .form()
        .unwrap()
        .prepare(state.stamp().unwrap())
        .unwrap();
    assert_eq!(state.track_request(&prepared), Err(StateError::Busy));
    assert!(state.pending_requests.is_empty());
    assert!(matches!(ctrl(&mut state, 's'), UiInput::None));
    state.close_form();
    assert_eq!(
        state.begin_switch(workspace("other")),
        Err(StateError::Busy)
    );
    state.commit_workspace(workspace("other"));
    assert!(state.detaching);
}

#[test]
fn dismissing_uncertain_workspace_creation_restores_use_without_resending() {
    let mut state = state();
    ctrl(&mut state, 'n');
    handle(&mut state, Event::Paste("unknown-room".to_owned()));
    let mut mutation = state.prepare_form(state.form().unwrap()).unwrap();
    mutation.submit().unwrap();
    mutation.mark_uncertain();
    state.mutation = Some(mutation);
    assert!(matches!(
        key(&mut state, KeyCode::Esc),
        UiInput::Command(UiCommand::LoadWorkspaces)
    ));
    assert!(state.form().is_none());
    assert!(state.mutation.is_none());
    assert_eq!(state.workspace.as_ref(), Some(&workspace("room")));
    assert_eq!(state.writable(), Ok(()));
    assert!(state.notice.is_some());
    // Ctrl-S cannot recreate the lost request, even if the new room was off-page.
    assert!(matches!(ctrl(&mut state, 's'), UiInput::None));
    ctrl(&mut state, 't');
    handle(&mut state, Event::Paste("unrelated task".to_owned()));
    assert!(matches!(
        ctrl(&mut state, 's'),
        UiInput::Command(UiCommand::SubmitForm)
    ));
    command_not_sent(&mut state, &UiCommand::SubmitForm);
    key(&mut state, KeyCode::Esc);
    assert_eq!(state.begin_switch(workspace("other")), Ok(()));
}

#[test]
fn dismissing_receipt_backed_uncertainty_retains_exact_retry_payload() {
    let mut state = state();
    load_task(&mut state, detail(1));
    key(&mut state, KeyCode::F(2));
    key(&mut state, KeyCode::Char('e'));
    set_field(state.form_mut().unwrap(), FieldId::Title, "immutable title");
    state.form_mut().unwrap().check_latest(&detail(1)).unwrap();
    let mut mutation = state.prepare_form(state.form().unwrap()).unwrap();
    let sent = mutation.submit().unwrap();
    mutation.mark_uncertain();
    state.mutation = Some(mutation);
    assert!(matches!(key(&mut state, KeyCode::Esc), UiInput::None));
    assert!(state.form().is_none());
    assert_eq!(state.writable(), Err(StateError::Busy));
    assert!(matches!(
        ctrl(&mut state, 's'),
        UiInput::Command(UiCommand::RetryMutation)
    ));
    assert_eq!(
        serde_json::to_value(state.mutation.as_mut().unwrap().retry().unwrap()).unwrap(),
        serde_json::to_value(sent).unwrap(),
    );
}

#[test]
fn canceling_reads_allows_new_members_load_without_changing_operation_authority() {
    let mut state = state();
    load_task(&mut state, detail(1));
    let stamp = state.stamp().unwrap();
    let old_members = state.begin_members_load().unwrap();
    let old_tasks = state.tasks.begin_load(stamp.clone());
    let old_workspaces = state.workspaces.begin_load();
    let old_history = state.chat.begin_history_load(stamp.clone());
    let mut form = FormState::new(FormKind::TaskEdit, Some(&detail(1))).unwrap();
    form.check_latest(&detail(1)).unwrap();
    let mut mutation = state.prepare_form(&form).unwrap();
    mutation.submit().unwrap();
    state.mutation = Some(mutation);
    state.invalidate_reads();
    assert_eq!(state.connection, Connection::Connected { epoch: 1 });
    assert_eq!(state.stamp(), Some(stamp));
    assert_eq!(
        state.mutation.as_ref().unwrap().stage,
        MutationStage::InFlight
    );
    assert!(state.chat.synchronized);
    let new_members = state.begin_members_load().unwrap();
    assert!(!state.apply_members(&old_members, Vec::new()));
    assert!(state.members_loading);
    assert!(state.apply_members(&new_members, Vec::new()));
    assert!(!state.apply_task_page(&old_tasks, vec![detail(2).summary], 1, false));
    assert!(
        !state
            .workspaces
            .apply(old_workspaces, Vec::new(), None, false)
    );
    assert!(
        state
            .apply_recent_page(&old_history, page(1, 2))
            .unwrap()
            .is_empty()
    );
    assert_eq!(state.tasks.selected_id, Some(1));
}
