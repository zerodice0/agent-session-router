//! Typed, transport-independent console state. Runtime completions must carry a
//! `WorkspaceStamp`; only commit a selection after the server confirms its join.

use std::collections::{BTreeMap, VecDeque};

use unicode_segmentation::UnicodeSegmentation;
use uuid::Uuid;

use crate::{
    protocol::{
        AgentDescriptor, ClientMessage, HistoryPage, MAX_AGENT_ID_BYTES, MAX_PAGE_LIMIT,
        MAX_SHARED_CONTENT_BYTES, MAX_WORKSPACE_NAME_BYTES, RouterErrorCode, TaskHistoryPage,
        WorkspaceEvent, WorkspaceEventKind, WorkspaceName, WorkspaceSummary, is_agent_id,
    },
    tasks::{
        AttemptStatus, MAX_DESCRIPTION_BYTES, MAX_HANDOFF_NOTE_BYTES, MAX_NOTE_BYTES,
        MAX_TITLE_BYTES, StopEvidence, TaskDetail, TaskEvent, TaskMutationResult, TaskState,
        TaskSummary, validate_description, validate_handoff_note, validate_note, validate_title,
    },
};

pub const MIN_WIDTH: u16 = 80;
pub const MIN_HEIGHT: u16 = 24;
pub const WIDE_WIDTH: u16 = 120;
pub const MAX_LIVE_EVENTS: usize = 2_000;
pub const MAX_HISTORY_EVENTS: usize = 100;
pub const MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_PENDING_REQUESTS: usize = 32;
pub const MAX_READS: usize = 4;
const MAX_PAGE_BACKTRACK: usize = 128;
const MAX_TASK_OVERLAYS: usize = 1_000;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Tab {
    #[default]
    Chat,
    Tasks,
    Members,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Focus {
    Workspaces,
    #[default]
    Main,
    Composer,
    Detail,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Connection {
    Connecting,
    Connected { epoch: u64 },
    Reconnecting,
    Closed { reason: Option<RouterErrorCode> },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Ownership {
    Owned,
    Reused,
    Remote,
}

/// Contains display metadata only, never credentials, paths or a runtime record.
#[derive(Clone, Debug)]
pub struct Header {
    pub profile: Option<String>,
    pub endpoint: String,
    pub transport: String,
    pub ownership: Ownership,
    pub instance_id: Option<Uuid>,
    pub is_admin: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceStamp {
    pub generation: u64,
    pub workspace: WorkspaceName,
}

/// Each independently refreshable resource has its own monotonically increasing
/// revision. Generations alone cannot reject an old page in the *same* room.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadTicket {
    pub stamp: WorkspaceStamp,
    pub revision: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SwitchPhase {
    Unsubscribing,
    Leaving,
    Joining,
    RecoveringPreviousMembership,
}

#[derive(Clone, Debug)]
pub struct WorkspaceSwitch {
    pub target: WorkspaceName,
    pub phase: SwitchPhase,
}

/// Bounded cursor navigation; a page number is not a claimed total page count.
#[derive(Clone, Debug)]
pub struct Pagination<C> {
    pub after: Option<C>,
    pub next_cursor: Option<C>,
    pub has_more: bool,
    pub page_number: usize,
    previous: VecDeque<Option<C>>,
}

impl<C: Clone> Default for Pagination<C> {
    fn default() -> Self {
        Self {
            after: None,
            next_cursor: None,
            has_more: false,
            page_number: 1,
            previous: VecDeque::new(),
        }
    }
}

impl<C: Clone> Pagination<C> {
    pub fn next_page(&mut self) -> bool {
        if !self.has_more || self.next_cursor.is_none() {
            return false;
        }
        if self.previous.len() == MAX_PAGE_BACKTRACK {
            self.previous.pop_front();
        }
        self.previous.push_back(self.after.clone());
        self.after.clone_from(&self.next_cursor);
        self.page_number = self.page_number.saturating_add(1);
        self.next_cursor = None;
        self.has_more = false;
        true
    }

    pub fn previous_page(&mut self) -> bool {
        let Some(after) = self.previous.pop_back() else {
            return false;
        };
        self.after = after;
        self.next_cursor = None;
        self.has_more = false;
        self.page_number = self.page_number.saturating_sub(1).max(1);
        true
    }

    pub fn has_previous(&self) -> bool {
        !self.previous.is_empty()
    }
}

#[derive(Clone, Debug, Default)]
pub struct WorkspacePage {
    pub rows: Vec<WorkspaceSummary>,
    pub selected: usize,
    pub pagination: Pagination<String>,
    revision: u64,
    pub loading: bool,
}

impl WorkspacePage {
    pub fn begin_load(&mut self) -> u64 {
        self.revision = self.revision.wrapping_add(1);
        self.loading = true;
        self.revision
    }

    pub fn apply(
        &mut self,
        revision: u64,
        rows: Vec<WorkspaceSummary>,
        next_cursor: Option<String>,
        has_more: bool,
    ) -> bool {
        if revision != self.revision || !self.loading {
            return false;
        }
        let selected = self.rows.get(self.selected).map(|row| row.name.clone());
        self.rows = rows.into_iter().take(usize::from(MAX_PAGE_LIMIT)).collect();
        self.selected = selected
            .and_then(|name| self.rows.iter().position(|row| row.name == name))
            .unwrap_or(self.selected.min(self.rows.len().saturating_sub(1)));
        self.pagination.next_cursor = next_cursor;
        self.pagination.has_more = has_more;
        self.loading = false;
        true
    }

    pub fn fail_load(&mut self, revision: u64) {
        if self.revision == revision {
            self.loading = false;
        }
    }
}

#[derive(Clone, Debug)]
pub struct EventBuffer {
    events: VecDeque<WorkspaceEvent>,
    bytes: usize,
    limit: usize,
    pub truncated: bool,
}

impl EventBuffer {
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            events: VecDeque::new(),
            bytes: 0,
            limit: limit.min(MAX_LIVE_EVENTS),
            truncated: false,
        }
    }

    #[must_use]
    pub fn events(&self) -> &VecDeque<WorkspaceEvent> {
        &self.events
    }
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn clear(&mut self) {
        self.events.clear();
        self.bytes = 0;
        self.truncated = false;
    }

    fn push(&mut self, event: WorkspaceEvent) {
        let bytes = event_bytes(&event);
        if bytes > MAX_EVENT_BYTES || self.limit == 0 {
            self.truncated = true;
            return;
        }
        while self.events.len() >= self.limit || self.bytes.saturating_add(bytes) > MAX_EVENT_BYTES
        {
            let Some(oldest) = self.events.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(event_bytes(&oldest));
            self.truncated = true;
        }
        self.bytes += bytes;
        self.events.push_back(event);
    }
}

fn event_bytes(event: &WorkspaceEvent) -> usize {
    std::mem::size_of::<WorkspaceEvent>()
        + event.workspace.as_str().len()
        + event.actor_id.capacity()
        + event.content.as_ref().map_or(0, String::capacity)
        + event.request_id.as_ref().map_or(0, String::capacity)
        + event.target_id.as_ref().map_or(0, String::capacity)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryMode {
    Live,
    Past,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DisplayCursor {
    pub next_cursor: i64,
    pub has_more: bool,
}

#[derive(Clone, Debug)]
pub struct ChatState {
    /// One body cache only: past mode never keeps a second live-tail cache.
    pub buffer: EventBuffer,
    pub mode: HistoryMode,
    pub follow: bool,
    pub selected_seq: Option<i64>,
    pub new_events: u64,
    pub all_events: bool,
    /// Live application/ACK cursor, independent of the visible history page.
    pub applied_cursor: Option<i64>,
    pub display: DisplayCursor,
    pub synchronized: bool,
    history_revision: u64,
}

impl Default for ChatState {
    fn default() -> Self {
        Self {
            buffer: EventBuffer::new(MAX_LIVE_EVENTS),
            mode: HistoryMode::Live,
            follow: true,
            selected_seq: None,
            new_events: 0,
            all_events: false,
            applied_cursor: None,
            display: DisplayCursor::default(),
            synchronized: false,
            history_revision: 0,
        }
    }
}

impl ChatState {
    pub fn begin_history_load(&mut self, stamp: WorkspaceStamp) -> ReadTicket {
        self.history_revision = self.history_revision.wrapping_add(1);
        ReadTicket {
            stamp,
            revision: self.history_revision,
        }
    }

    pub fn enter_past(&mut self) {
        self.mode = HistoryMode::Past;
        self.follow = false;
        self.buffer = EventBuffer::new(MAX_HISTORY_EVENTS);
        self.selected_seq = None;
    }

    /// The runtime must fetch a fresh recent page for End, not replay a stale cache.
    pub fn begin_return_to_live(&mut self) {
        self.history_revision = self.history_revision.wrapping_add(1);
        self.mode = HistoryMode::Live;
        self.follow = true;
        self.buffer = EventBuffer::new(MAX_LIVE_EVENTS);
        self.selected_seq = None;
        self.new_events = 0;
    }

    #[must_use]
    pub fn visible(&self, event: &WorkspaceEvent) -> bool {
        self.all_events
            || matches!(
                event.kind,
                WorkspaceEventKind::Chat
                    | WorkspaceEventKind::Request
                    | WorkspaceEventKind::Result
                    | WorkspaceEventKind::Task
            )
    }

    pub fn select_relative(&mut self, delta: isize) {
        let mut current = None;
        let mut count: usize = 0;
        for event in self
            .buffer
            .events()
            .iter()
            .filter(|event| self.visible(event))
        {
            if Some(event.seq) == self.selected_seq {
                current = Some(count);
            }
            count += 1;
        }
        if count == 0 {
            self.selected_seq = None;
            return;
        }
        let index = current
            .unwrap_or(count - 1)
            .saturating_add_signed(delta)
            .min(count - 1);
        self.selected_seq = self
            .buffer
            .events()
            .iter()
            .filter(|event| self.visible(event))
            .nth(index)
            .map(|event| event.seq);
        self.follow = false;
    }
}

fn valid_history_page(page: &HistoryPage, workspace: &WorkspaceName) -> bool {
    page.workspace == *workspace
        && page.next_cursor >= 0
        && page.events.len() <= MAX_HISTORY_EVENTS
        && page
            .events
            .iter()
            .all(|event| event.workspace == *workspace && event.seq > 0)
        && page
            .events
            .windows(2)
            .all(|events| events[0].seq.checked_add(1) == Some(events[1].seq))
        && page
            .events
            .last()
            .is_none_or(|event| event.seq == page.next_cursor)
}

/// Collects a previous 100-event page through the protocol's forward-only API.
/// A short byte-capped response is not treated as the end of the requested range.
#[derive(Clone, Debug)]
pub struct PreviousHistoryLoad {
    pub boundary: i64,
    pub after: i64,
    pub buffer: EventBuffer,
    pub done: bool,
}

impl PreviousHistoryLoad {
    #[must_use]
    pub fn new(oldest_seq: i64) -> Self {
        Self {
            boundary: oldest_seq,
            after: oldest_seq.saturating_sub(101).max(0),
            buffer: EventBuffer::new(MAX_HISTORY_EVENTS),
            done: oldest_seq <= 1,
        }
    }

    pub fn absorb(&mut self, page: HistoryPage) -> Result<(), StateError> {
        if self.done {
            return Ok(());
        }
        if !valid_history_page(&page, &page.workspace) || page.next_cursor < self.after {
            return Err(StateError::InvalidPage);
        }
        let previous_after = self.after;
        for event in page.events {
            if event.seq >= self.boundary {
                self.done = true;
                break;
            }
            if event.seq > self.after {
                self.after = event.seq;
                self.buffer.push(event);
            }
        }
        self.done |= !page.has_more || page.next_cursor >= self.boundary.saturating_sub(1);
        if !self.done {
            self.after = self.after.max(page.next_cursor);
            if self.after <= previous_after {
                return Err(StateError::InvalidPage);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskFilter {
    pub states: Vec<TaskState>,
    pub assigned_agent_id: Option<String>,
}

impl Default for TaskFilter {
    fn default() -> Self {
        Self {
            states: vec![
                TaskState::Todo,
                TaskState::InProgress,
                TaskState::Blocked,
                TaskState::Paused,
                TaskState::Done,
                TaskState::Cancelled,
            ],
            assigned_agent_id: None,
        }
    }
}

impl TaskFilter {
    fn matches(&self, task: &TaskSummary) -> bool {
        self.states.contains(&task.state)
            && self
                .assigned_agent_id
                .as_ref()
                .is_none_or(|agent| task.assigned_agent_id.as_ref() == Some(agent))
    }
}

#[derive(Clone, Debug, Default)]
pub struct TaskPage {
    pub rows: BTreeMap<i64, TaskSummary>,
    pub selected_id: Option<i64>,
    pub selected_summary: Option<TaskSummary>,
    pub detail: Option<TaskDetail>,
    pub detail_stale: bool,
    pub filter: TaskFilter,
    pub pagination: Pagination<i64>,
    pub history: Option<TaskHistoryPage>,
    pub history_pagination: Pagination<i64>,
    pub refresh_required: bool,
    list_revision: u64,
    list_in_flight: bool,
    detail_revision: u64,
    history_revision: u64,
    overlays: BTreeMap<i64, TaskSummary>,
}

impl TaskPage {
    pub fn set_filter(&mut self, filter: TaskFilter) {
        self.filter = filter;
        self.pagination = Pagination::default();
        self.rows.clear();
        self.select(None);
        self.invalidate_reads();
        self.refresh_required = true;
    }

    fn invalidate_reads(&mut self) {
        self.list_revision = self.list_revision.wrapping_add(1);
        self.detail_revision = self.detail_revision.wrapping_add(1);
        self.history_revision = self.history_revision.wrapping_add(1);
        self.list_in_flight = false;
        self.overlays.clear();
        self.detail_stale = self.selected_id.is_some();
    }

    pub fn begin_load(&mut self, stamp: WorkspaceStamp) -> ReadTicket {
        self.list_revision = self.list_revision.wrapping_add(1);
        self.list_in_flight = true;
        self.overlays.clear();
        ReadTicket {
            stamp,
            revision: self.list_revision,
        }
    }

    pub fn fail_load(&mut self, revision: u64) {
        if revision == self.list_revision {
            self.list_in_flight = false;
            self.overlays.clear();
        }
    }

    pub fn apply_page(
        &mut self,
        revision: u64,
        tasks: Vec<TaskSummary>,
        next_cursor: i64,
        has_more: bool,
    ) -> bool {
        if revision != self.list_revision || !self.list_in_flight {
            return false;
        }
        let mut rows = BTreeMap::new();
        for mut task in tasks.into_iter().take(usize::from(MAX_PAGE_LIMIT)) {
            for newer in [
                self.rows.get(&task.id),
                self.overlays.get(&task.id),
                self.selected_summary
                    .as_ref()
                    .filter(|selected| selected.id == task.id),
            ]
            .into_iter()
            .flatten()
            {
                if newer.version > task.version {
                    task = newer.clone();
                }
            }
            if self.filter.matches(&task) {
                rows.insert(task.id, task);
            }
        }
        for task in self.overlays.values() {
            if task.id > self.pagination.after.unwrap_or(0)
                && (task.id <= next_cursor || !has_more)
                && self.filter.matches(task)
                && rows
                    .get(&task.id)
                    .is_none_or(|old| old.version < task.version)
            {
                rows.insert(task.id, task.clone());
            }
        }
        let overflow = rows.len() > usize::from(MAX_PAGE_LIMIT);
        while rows.len() > usize::from(MAX_PAGE_LIMIT) {
            rows.pop_last();
        }
        self.rows = rows;
        self.pagination.next_cursor = Some(if overflow {
            self.rows
                .last_key_value()
                .map_or(next_cursor, |(id, _)| *id)
        } else {
            next_cursor
        });
        self.pagination.has_more = has_more || overflow;
        self.list_in_flight = false;
        self.overlays.clear();
        self.refresh_required = false;
        if self.selected_id.is_none() {
            self.select(self.rows.first_key_value().map(|(id, _)| *id));
        } else if let Some(task) = self.selected_id.and_then(|id| self.rows.get(&id)).cloned() {
            self.observe_selected(task);
        }
        true
    }

    pub fn observe(&mut self, task: TaskSummary) -> bool {
        let id = task.id;
        let stale = self
            .rows
            .get(&id)
            .is_some_and(|old| old.version > task.version)
            || self
                .selected_summary
                .as_ref()
                .is_some_and(|old| old.id == id && old.version > task.version)
            || self
                .overlays
                .get(&id)
                .is_some_and(|old| old.version > task.version);
        if stale {
            return false;
        }
        if self.list_in_flight {
            if self.overlays.len() >= MAX_TASK_OVERLAYS && !self.overlays.contains_key(&id) {
                // Never evict a newer version and then accept its stale snapshot.
                self.list_in_flight = false;
                self.list_revision = self.list_revision.wrapping_add(1);
                self.overlays.clear();
                self.refresh_required = true;
            } else {
                self.overlays.insert(id, task.clone());
            }
        }
        let selected = self.selected_id == Some(id);
        if selected {
            self.observe_selected(task.clone());
        }
        if self.rows.contains_key(&id) {
            if self.filter.matches(&task) {
                self.rows.insert(id, task);
            } else {
                self.rows.remove(&id);
            }
        } else {
            // A new/moved row may alter pagination; refresh instead of inventing
            // a position or inserting beyond the authoritative page boundary.
            self.refresh_required = true;
        }
        selected && self.detail_stale
    }

    fn observe_selected(&mut self, task: TaskSummary) {
        if self
            .selected_summary
            .as_ref()
            .is_some_and(|old| old.id == task.id && old.version > task.version)
        {
            return;
        }
        self.detail_stale = self.detail.as_ref().is_none_or(|detail| {
            detail.summary.id != task.id || detail.summary.version < task.version
        });
        self.selected_summary = Some(task);
    }

    pub fn select(&mut self, id: Option<i64>) {
        if self.selected_id == id {
            return;
        }
        self.selected_id = id;
        self.selected_summary = id.and_then(|id| self.rows.get(&id).cloned());
        self.detail = None;
        self.detail_stale = id.is_some();
        self.history = None;
        self.history_pagination = Pagination::default();
        self.detail_revision = self.detail_revision.wrapping_add(1);
        self.history_revision = self.history_revision.wrapping_add(1);
    }

    pub fn select_relative(&mut self, delta: isize) {
        if self.rows.is_empty() {
            self.select(None);
            return;
        }
        let current = self
            .rows
            .keys()
            .position(|id| Some(*id) == self.selected_id)
            .unwrap_or(0);
        let index = current
            .saturating_add_signed(delta)
            .min(self.rows.len() - 1);
        self.select(self.rows.keys().nth(index).copied());
    }

    pub fn begin_detail_load(&mut self, stamp: WorkspaceStamp) -> ReadTicket {
        self.detail_revision = self.detail_revision.wrapping_add(1);
        ReadTicket {
            stamp,
            revision: self.detail_revision,
        }
    }

    pub fn apply_detail(&mut self, revision: u64, detail: TaskDetail) -> bool {
        if revision != self.detail_revision || self.selected_id != Some(detail.summary.id) {
            return false;
        }
        if self
            .selected_summary
            .as_ref()
            .is_some_and(|task| task.version > detail.summary.version)
            || self
                .rows
                .get(&detail.summary.id)
                .is_some_and(|task| task.version > detail.summary.version)
        {
            self.detail_stale = true;
            return false;
        }
        self.observe(detail.summary.clone());
        self.detail = Some(detail);
        self.detail_stale = false;
        true
    }

    pub fn begin_history_load(&mut self, stamp: WorkspaceStamp) -> ReadTicket {
        self.history_revision = self.history_revision.wrapping_add(1);
        ReadTicket {
            stamp,
            revision: self.history_revision,
        }
    }

    pub fn apply_history(&mut self, revision: u64, mut page: TaskHistoryPage) -> bool {
        if revision != self.history_revision || Some(page.task_id) != self.selected_id {
            return false;
        }
        page.events.truncate(usize::from(MAX_PAGE_LIMIT));
        // Task history is for display, never an authoritative current snapshot.
        self.history_pagination.next_cursor = Some(page.next_cursor);
        self.history_pagination.has_more = page.has_more;
        self.history = Some(page);
        true
    }

    pub fn can_request(&self) -> bool {
        !self.detail_stale && self.detail.as_ref().is_some_and(requestable_task)
    }

    #[must_use]
    pub fn confirmable_attempt(&self) -> Option<Uuid> {
        if self.detail_stale {
            return None;
        }
        self.detail.as_ref().and_then(confirmable_attempt)
    }
}

fn requestable_task(detail: &TaskDetail) -> bool {
    detail.summary.state.is_beginable()
        && detail.summary.assigned_agent_id.is_some()
        && detail.summary.current_attempt_id.is_none()
        && detail.current_attempt.is_none()
        && detail.summary.stop_evidence != Some(StopEvidence::Unknown)
}

#[must_use]
pub fn confirmable_attempt(detail: &TaskDetail) -> Option<Uuid> {
    if detail.current_attempt.is_some()
        || detail.summary.current_attempt_id.is_some()
        || detail.summary.stop_evidence != Some(StopEvidence::Unknown)
    {
        return None;
    }
    detail
        .last_attempt
        .as_ref()
        .filter(|attempt| {
            attempt.status == AttemptStatus::Interrupted
                && attempt.stop_evidence == StopEvidence::Unknown
        })
        .map(|attempt| attempt.id)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateError {
    NoWorkspace,
    Busy,
    NotConnected,
    Synchronizing,
    TerminalTooSmall,
    PermissionDenied,
    InvalidPage,
    InvalidInput,
    InputTooLarge,
    SingleLine,
    MissingTask,
    ReconfirmationRequired,
    MustObserveStopped,
    StopUnconfirmed,
    RequestNotRetryable,
    NotUncertain,
    WrongReceipt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventDisposition {
    Applied,
    Duplicate,
    StaleWorkspace,
    Gap { expected: i64, received: i64 },
    Invalid,
}

#[derive(Clone, Debug)]
pub struct EventApplied {
    pub disposition: EventDisposition,
    /// ACK only after this method returns, never on receipt from the socket.
    pub ack: Option<i64>,
    pub refresh_task_detail: bool,
    pub request_resolved: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestStage {
    Sending,
    Accepted,
    ExecutionObserved,
    Uncertain,
}

#[derive(Clone, Debug)]
pub struct PendingRequest {
    pub request_id: String,
    pub task_id: i64,
    pub expected_version: i64,
    pub stage: RequestStage,
}

#[derive(Clone, Debug)]
pub struct Composer {
    pub input: InputBuffer,
    pending: Option<PendingPost>,
}

#[derive(Clone, Debug)]
pub struct PendingPost {
    request_id: String,
    content: String,
    pub uncertain: bool,
}

impl Default for Composer {
    fn default() -> Self {
        Self {
            input: InputBuffer::new(MAX_SHARED_CONTENT_BYTES, true),
            pending: None,
        }
    }
}

impl Composer {
    #[must_use]
    pub fn pending(&self) -> Option<&PendingPost> {
        self.pending.as_ref()
    }
    #[must_use]
    pub fn is_locked(&self) -> bool {
        self.pending.is_some()
    }

    pub fn prepare(&mut self) -> Result<ClientMessage, StateError> {
        if self.pending.is_some() {
            return Err(StateError::Busy);
        }
        if self.input.text().is_empty() {
            return Err(StateError::InvalidInput);
        }
        let request_id = Uuid::new_v4().to_string();
        let content = self.input.text().to_owned();
        let message = ClientMessage::WorkspacePost {
            request_id: request_id.clone(),
            content: content.clone(),
        };
        self.pending = Some(PendingPost {
            request_id,
            content,
            uncertain: false,
        });
        Ok(message)
    }

    pub fn mark_uncertain(&mut self) {
        if let Some(post) = self.pending.as_mut() {
            post.uncertain = true;
        }
    }

    pub fn retry(&mut self) -> Result<ClientMessage, StateError> {
        let post = self
            .pending
            .as_mut()
            .filter(|post| post.uncertain)
            .ok_or(StateError::NotUncertain)?;
        post.uncertain = false;
        Ok(ClientMessage::WorkspacePost {
            request_id: post.request_id.clone(),
            content: post.content.clone(),
        })
    }

    pub fn acknowledge(&mut self, request_id: &str) -> bool {
        let Some(post) = self
            .pending
            .as_ref()
            .filter(|post| post.request_id == request_id)
        else {
            return false;
        };
        if self.input.text() == post.content {
            self.input.clear();
        }
        self.pending = None;
        true
    }

    pub fn reject(&mut self, request_id: &str) {
        if self
            .pending
            .as_ref()
            .is_some_and(|post| post.request_id == request_id)
        {
            self.pending = None;
        }
    }
}

/// A cursor is a UTF-8 offset, always normalized to an extended grapheme boundary.
#[derive(Clone, Debug)]
pub struct InputBuffer {
    text: String,
    cursor: usize,
    max_bytes: usize,
    multiline: bool,
}

impl InputBuffer {
    #[must_use]
    pub fn new(max_bytes: usize, multiline: bool) -> Self {
        Self {
            text: String::new(),
            cursor: 0,
            max_bytes,
            multiline,
        }
    }

    pub fn with_text(text: &str, max_bytes: usize, multiline: bool) -> Result<Self, StateError> {
        let mut input = Self::new(max_bytes, multiline);
        input.insert(text)?;
        Ok(input)
    }

    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }
    #[must_use]
    pub fn cursor(&self) -> usize {
        self.cursor
    }
    #[must_use]
    pub fn multiline(&self) -> bool {
        self.multiline
    }
    #[must_use]
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }
    pub fn clear(&mut self) {
        self.text.clear();
        self.cursor = 0;
    }

    /// Bracketed paste uses the same insertion path; it can never submit a form.
    pub fn insert(&mut self, text: &str) -> Result<(), StateError> {
        if text.len() > self.max_bytes.saturating_mul(2) {
            return Err(StateError::InputTooLarge);
        }
        let normalized;
        let text = if text.contains('\r') {
            normalized = text.replace("\r\n", "\n").replace('\r', "\n");
            &normalized
        } else {
            text
        };
        if !self.multiline && text.contains('\n') {
            return Err(StateError::SingleLine);
        }
        if self.text.len().saturating_add(text.len()) > self.max_bytes {
            return Err(StateError::InputTooLarge);
        }
        self.text.insert_str(self.cursor, text);
        self.cursor += text.len();
        self.normalize_cursor_forward();
        Ok(())
    }

    fn normalize_cursor_forward(&mut self) {
        self.cursor = self
            .text
            .grapheme_indices(true)
            .map(|(offset, _)| offset)
            .find(|offset| *offset >= self.cursor)
            .unwrap_or(self.text.len());
    }

    pub fn left(&mut self) {
        self.cursor = self.text[..self.cursor]
            .grapheme_indices(true)
            .next_back()
            .map_or(0, |(offset, _)| offset);
    }

    pub fn right(&mut self) {
        if let Some(grapheme) = self.text[self.cursor..].graphemes(true).next() {
            self.cursor += grapheme.len();
        }
    }

    pub fn backspace(&mut self) {
        let end = self.cursor;
        self.left();
        self.text.replace_range(self.cursor..end, "");
        self.normalize_cursor_forward();
    }

    pub fn delete(&mut self) {
        if let Some(grapheme) = self.text[self.cursor..].graphemes(true).next() {
            let end = self.cursor + grapheme.len();
            self.text.replace_range(self.cursor..end, "");
            self.normalize_cursor_forward();
        }
    }

    pub fn home(&mut self) {
        self.cursor = self.text[..self.cursor]
            .rfind('\n')
            .map_or(0, |offset| offset + 1);
    }
    pub fn end(&mut self) {
        self.cursor += self.text[self.cursor..]
            .find('\n')
            .unwrap_or(self.text.len() - self.cursor);
    }

    pub fn vertical(&mut self, down: bool) {
        let start = self.text[..self.cursor]
            .rfind('\n')
            .map_or(0, |offset| offset + 1);
        let column = self.text[start..self.cursor].graphemes(true).count();
        let range = if down {
            let Some(end) = self.text[self.cursor..]
                .find('\n')
                .map(|offset| self.cursor + offset)
            else {
                return;
            };
            let next_start = end + 1;
            let next_end = self.text[next_start..]
                .find('\n')
                .map_or(self.text.len(), |offset| next_start + offset);
            next_start..next_end
        } else {
            if start == 0 {
                return;
            }
            let previous_end = start - 1;
            let previous_start = self.text[..previous_end]
                .rfind('\n')
                .map_or(0, |offset| offset + 1);
            previous_start..previous_end
        };
        self.cursor = self.text[range.clone()]
            .grapheme_indices(true)
            .nth(column)
            .map_or(range.end, |(offset, _)| range.start + offset);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FormKind {
    WorkspaceCreate,
    TaskCreate,
    TaskEdit,
    TaskAssign,
    TaskRequest,
    TaskInterrupt,
    TaskConfirmStopped,
    TaskNote,
    TaskCancel,
    TaskReopen,
    TaskFilter,
    Invite,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldId {
    Name,
    Title,
    Description,
    Agent,
    Message,
    Note,
    States,
    Profile,
    Provider,
    Endpoints,
    CaFile,
}

#[derive(Clone, Debug)]
pub struct FormField {
    pub id: FieldId,
    pub label: &'static str,
    pub input: InputBuffer,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskBaseline {
    pub workspace: String,
    pub task_id: i64,
    pub version: i64,
    pub attempt_id: Option<Uuid>,
    pub assigned_agent_id: Option<String>,
}

impl TaskBaseline {
    #[must_use]
    pub fn from_detail(detail: &TaskDetail) -> Self {
        Self {
            workspace: detail.summary.workspace.clone(),
            task_id: detail.summary.id,
            version: detail.summary.version,
            attempt_id: detail
                .current_attempt
                .as_ref()
                .or(detail.last_attempt.as_ref())
                .map(|attempt| attempt.id),
            assigned_agent_id: detail.summary.assigned_agent_id.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FormState {
    pub kind: FormKind,
    pub fields: Vec<FormField>,
    pub focused: usize,
    pub baseline: Option<TaskBaseline>,
    pub changed: Option<TaskBaseline>,
    pub stopped_observed: bool,
    pub assignee_choice: Option<usize>,
    pub error: Option<StateError>,
    pub router_error: Option<RouterErrorCode>,
    baseline_checked: bool,
}

impl FormState {
    pub fn new(kind: FormKind, detail: Option<&TaskDetail>) -> Result<Self, StateError> {
        let needs_task = !matches!(
            kind,
            FormKind::WorkspaceCreate
                | FormKind::TaskCreate
                | FormKind::TaskFilter
                | FormKind::Invite
        );
        if needs_task && detail.is_none() {
            return Err(StateError::MissingTask);
        }
        let mut form = Self {
            kind,
            fields: Vec::new(),
            focused: 0,
            baseline: if needs_task {
                detail.map(TaskBaseline::from_detail)
            } else {
                None
            },
            changed: None,
            stopped_observed: false,
            assignee_choice: None,
            error: None,
            router_error: None,
            baseline_checked: false,
        };
        match kind {
            FormKind::WorkspaceCreate => form.field(
                FieldId::Name,
                "Workspace name",
                "",
                MAX_WORKSPACE_NAME_BYTES,
                false,
            )?,
            FormKind::TaskCreate | FormKind::TaskEdit => {
                let detail = if kind == FormKind::TaskEdit {
                    detail
                } else {
                    None
                };
                form.field(
                    FieldId::Title,
                    "Title",
                    detail.map_or("", |task| task.summary.title.as_str()),
                    MAX_TITLE_BYTES,
                    false,
                )?;
                form.field(
                    FieldId::Description,
                    "Description",
                    detail.map_or("", |task| task.description.as_str()),
                    MAX_DESCRIPTION_BYTES,
                    true,
                )?;
            }
            FormKind::TaskAssign => form.field(
                FieldId::Agent,
                "Assignee ID (empty = unassigned)",
                detail
                    .and_then(|task| task.summary.assigned_agent_id.as_deref())
                    .unwrap_or(""),
                MAX_AGENT_ID_BYTES,
                false,
            )?,
            FormKind::TaskRequest => form.field(
                FieldId::Message,
                "Optional execution request",
                "",
                MAX_SHARED_CONTENT_BYTES,
                true,
            )?,
            FormKind::TaskInterrupt | FormKind::TaskCancel | FormKind::TaskReopen => form.field(
                FieldId::Note,
                "Required handoff note",
                "",
                MAX_HANDOFF_NOTE_BYTES,
                true,
            )?,
            FormKind::TaskConfirmStopped => {
                let task = detail.ok_or(StateError::MissingTask)?;
                if confirmable_attempt(task).is_none() {
                    return Err(StateError::StopUnconfirmed);
                }
                form.field(
                    FieldId::Note,
                    "Required observed-stop note",
                    "",
                    MAX_HANDOFF_NOTE_BYTES,
                    true,
                )?;
            }
            FormKind::TaskNote => {
                form.field(FieldId::Note, "Required note", "", MAX_NOTE_BYTES, true)?;
            }
            FormKind::TaskFilter => {
                form.field(
                    FieldId::States,
                    "States (comma-separated; empty = all six)",
                    "",
                    128,
                    false,
                )?;
                form.field(
                    FieldId::Agent,
                    "Assignee ID (empty = all)",
                    "",
                    MAX_AGENT_ID_BYTES,
                    false,
                )?;
            }
            FormKind::Invite => {
                form.field(FieldId::Profile, "Profile name (optional)", "", 128, false)?;
                form.field(
                    FieldId::Provider,
                    "Provider: claude-code / codex-cli / omp (optional)",
                    "",
                    16,
                    false,
                )?;
                form.field(
                    FieldId::Endpoints,
                    "Optional endpoints: KIND=URL, one per line",
                    "",
                    8 * 1024,
                    true,
                )?;
                form.field(
                    FieldId::CaFile,
                    "Optional public CA file",
                    "",
                    4 * 1024,
                    false,
                )?;
            }
        }
        Ok(form)
    }

    fn field(
        &mut self,
        id: FieldId,
        label: &'static str,
        value: &str,
        max_bytes: usize,
        multiline: bool,
    ) -> Result<(), StateError> {
        self.fields.push(FormField {
            id,
            label,
            input: InputBuffer::with_text(value, max_bytes, multiline)?,
        });
        Ok(())
    }

    #[must_use]
    pub fn value(&self, id: FieldId) -> &str {
        self.fields
            .iter()
            .find(|field| field.id == id)
            .map_or("", |field| field.input.text())
    }
    pub fn focused_input(&mut self) -> Option<&mut InputBuffer> {
        self.fields
            .get_mut(self.focused)
            .map(|field| &mut field.input)
    }
    pub fn next_field(&mut self, backwards: bool) {
        // Confirm-stopped adds a focusable checkbox after the text fields.
        let count = self.fields.len() + usize::from(self.kind == FormKind::TaskConfirmStopped);
        if count > 0 {
            self.focused = if backwards {
                (self.focused + count - 1) % count
            } else {
                (self.focused + 1) % count
            };
        }
    }

    /// None is explicitly unassigned. A direct typed ID remains supported and is
    /// validated by the server even when that identity is currently offline.
    pub fn choose_assignee(
        &mut self,
        members: &[AgentDescriptor],
        index: Option<usize>,
    ) -> Result<(), StateError> {
        if self.kind != FormKind::TaskAssign {
            return Err(StateError::InvalidInput);
        }
        let value = match index {
            Some(index) => members
                .get(index)
                .ok_or(StateError::InvalidInput)?
                .agent_id
                .as_str(),
            None => "",
        };
        let field = self
            .fields
            .iter_mut()
            .find(|field| field.id == FieldId::Agent)
            .ok_or(StateError::InvalidInput)?;
        field.input = InputBuffer::with_text(value, MAX_AGENT_ID_BYTES, false)?;
        self.assignee_choice = index;
        Ok(())
    }

    pub fn enter(&mut self) -> Result<(), StateError> {
        if let Some(input) = self.focused_input().filter(|input| input.multiline()) {
            input.insert("\n")
        } else {
            self.next_field(false);
            Ok(())
        }
    }

    pub fn toggle_checkbox(&mut self) {
        if self.kind == FormKind::TaskConfirmStopped && self.focused == self.fields.len() {
            self.stopped_observed = !self.stopped_observed;
        }
    }

    /// Called only after a fresh `task_get`, before allocating/submitting an operation.
    pub fn check_latest(&mut self, latest: &TaskDetail) -> Result<(), StateError> {
        self.baseline_checked = false;
        let baseline = self.baseline.as_ref().ok_or(StateError::MissingTask)?;
        if latest.summary.id != baseline.task_id || latest.summary.workspace != baseline.workspace {
            return Err(StateError::MissingTask);
        }
        if self.kind == FormKind::TaskNote {
            return Ok(());
        }
        let current = TaskBaseline::from_detail(latest);
        if *baseline != current {
            self.changed = Some(current);
            self.error = Some(StateError::ReconfirmationRequired);
            return Err(StateError::ReconfirmationRequired);
        }
        if self.kind == FormKind::TaskConfirmStopped
            && confirmable_attempt(latest) != baseline.attempt_id
        {
            return Err(StateError::StopUnconfirmed);
        }
        if self.kind == FormKind::TaskRequest && !requestable_task(latest) {
            return Err(StateError::StopUnconfirmed);
        }
        self.baseline_checked = true;
        Ok(())
    }

    /// Explicit user action only. Keeps the draft text, but any prepared operation
    /// must be discarded; preparing again creates a new immutable operation ID.
    pub fn reconfirm_latest(&mut self) -> Result<(), StateError> {
        self.baseline = Some(
            self.changed
                .take()
                .ok_or(StateError::ReconfirmationRequired)?,
        );
        self.stopped_observed = false;
        self.baseline_checked = false;
        self.error = None;
        self.router_error = None;
        Ok(())
    }

    pub fn task_filter(&self) -> Result<TaskFilter, StateError> {
        let mut filter = TaskFilter::default();
        if !self.value(FieldId::States).trim().is_empty() {
            filter.states.clear();
            for word in self.value(FieldId::States).split(',').map(str::trim) {
                let state = match word {
                    "todo" => TaskState::Todo,
                    "in_progress" => TaskState::InProgress,
                    "blocked" => TaskState::Blocked,
                    "paused" => TaskState::Paused,
                    "done" => TaskState::Done,
                    "cancelled" => TaskState::Cancelled,
                    _ => return Err(StateError::InvalidInput),
                };
                if !filter.states.contains(&state) {
                    filter.states.push(state);
                }
            }
        }
        let agent = self.value(FieldId::Agent);
        if !agent.is_empty() && !is_agent_id(agent) {
            return Err(StateError::InvalidInput);
        }
        filter.assigned_agent_id = (!agent.is_empty()).then(|| agent.to_owned());
        Ok(filter)
    }

    pub fn prepare(&self, stamp: WorkspaceStamp) -> Result<PreparedMutation, StateError> {
        if self.changed.is_some() {
            return Err(StateError::ReconfirmationRequired);
        }
        if !matches!(
            self.kind,
            FormKind::WorkspaceCreate
                | FormKind::TaskCreate
                | FormKind::TaskNote
                | FormKind::TaskFilter
                | FormKind::Invite
        ) && !self.baseline_checked
        {
            return Err(StateError::ReconfirmationRequired);
        }
        if self
            .baseline
            .as_ref()
            .is_some_and(|baseline| baseline.workspace != stamp.workspace.as_str())
        {
            return Err(StateError::MissingTask);
        }
        let request_id = Uuid::new_v4().to_string();
        let operation_id = Uuid::new_v4();
        let workspace = stamp.workspace.clone();
        let task_id = self
            .baseline
            .as_ref()
            .map_or(0, |baseline| baseline.task_id);
        let version = self
            .baseline
            .as_ref()
            .map_or(0, |baseline| baseline.version);
        let text = |id| self.value(id).to_owned();
        let invalid = |_| StateError::InvalidInput;
        if self.kind == FormKind::TaskAssign
            && !self.value(FieldId::Agent).is_empty()
            && !is_agent_id(self.value(FieldId::Agent))
        {
            return Err(StateError::InvalidInput);
        }
        let message = match self.kind {
            FormKind::WorkspaceCreate => ClientMessage::WorkspaceCreate {
                request_id: request_id.clone(),
                name: WorkspaceName::parse(text(FieldId::Name))
                    .map_err(|_| StateError::InvalidInput)?,
            },
            FormKind::TaskCreate => {
                validate_title(self.value(FieldId::Title)).map_err(invalid)?;
                validate_description(self.value(FieldId::Description)).map_err(invalid)?;
                ClientMessage::TaskCreate {
                    request_id: request_id.clone(),
                    workspace,
                    operation_id,
                    title: text(FieldId::Title),
                    description: text(FieldId::Description),
                }
            }
            FormKind::TaskEdit => {
                validate_title(self.value(FieldId::Title)).map_err(invalid)?;
                validate_description(self.value(FieldId::Description)).map_err(invalid)?;
                ClientMessage::TaskEdit {
                    request_id: request_id.clone(),
                    workspace,
                    operation_id,
                    task_id,
                    expected_version: version,
                    title: Some(text(FieldId::Title)),
                    description: Some(text(FieldId::Description)),
                }
            }
            FormKind::TaskAssign => ClientMessage::TaskAssign {
                request_id: request_id.clone(),
                workspace,
                operation_id,
                task_id,
                expected_version: version,
                agent_id: (!self.value(FieldId::Agent).is_empty()).then(|| text(FieldId::Agent)),
            },
            FormKind::TaskRequest => ClientMessage::TaskRequest {
                request_id: request_id.clone(),
                workspace,
                task_id,
                expected_version: version,
                message: (!self.value(FieldId::Message).is_empty()).then(|| text(FieldId::Message)),
                timeout_ms: None,
            },
            FormKind::TaskInterrupt => {
                validate_handoff_note(self.value(FieldId::Note)).map_err(invalid)?;
                ClientMessage::TaskInterrupt {
                    request_id: request_id.clone(),
                    workspace,
                    operation_id,
                    task_id,
                    expected_version: version,
                    note: text(FieldId::Note),
                }
            }
            FormKind::TaskConfirmStopped => {
                if !self.stopped_observed {
                    return Err(StateError::MustObserveStopped);
                }
                validate_handoff_note(self.value(FieldId::Note)).map_err(invalid)?;
                ClientMessage::TaskConfirmStopped {
                    request_id: request_id.clone(),
                    workspace,
                    operation_id,
                    task_id,
                    attempt_id: self
                        .baseline
                        .as_ref()
                        .and_then(|baseline| baseline.attempt_id)
                        .ok_or(StateError::StopUnconfirmed)?,
                    expected_version: version,
                    note: text(FieldId::Note),
                }
            }
            FormKind::TaskNote => {
                validate_note(self.value(FieldId::Note)).map_err(invalid)?;
                ClientMessage::TaskNote {
                    request_id: request_id.clone(),
                    workspace,
                    operation_id,
                    task_id,
                    text: text(FieldId::Note),
                }
            }
            FormKind::TaskCancel | FormKind::TaskReopen => {
                validate_handoff_note(self.value(FieldId::Note)).map_err(invalid)?;
                if self.kind == FormKind::TaskCancel {
                    ClientMessage::TaskCancel {
                        request_id: request_id.clone(),
                        workspace,
                        operation_id,
                        task_id,
                        expected_version: version,
                        note: text(FieldId::Note),
                    }
                } else {
                    ClientMessage::TaskReopen {
                        request_id: request_id.clone(),
                        workspace,
                        operation_id,
                        task_id,
                        expected_version: version,
                        note: text(FieldId::Note),
                    }
                }
            }
            FormKind::TaskFilter | FormKind::Invite => return Err(StateError::InvalidInput),
        };
        let operation_id =
            (!matches!(self.kind, FormKind::WorkspaceCreate | FormKind::TaskRequest))
                .then_some(operation_id);
        Ok(PreparedMutation {
            stamp,
            kind: self.kind,
            request_id,
            operation_id,
            message,
            baseline: self.baseline.clone(),
            stage: MutationStage::Prepared,
            current_version: None,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationStage {
    Prepared,
    InFlight,
    Uncertain,
    Conflict,
    Applied,
    Rejected,
}

/// The wire payload is private and never edited after allocation. Retrying clones
/// exactly that payload, including both IDs; reconfirmation creates a new object.
#[derive(Clone)]
pub struct PreparedMutation {
    pub stamp: WorkspaceStamp,
    pub kind: FormKind,
    request_id: String,
    operation_id: Option<Uuid>,
    message: ClientMessage,
    baseline: Option<TaskBaseline>,
    pub stage: MutationStage,
    pub current_version: Option<i64>,
}

impl PreparedMutation {
    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    #[must_use]
    pub fn operation_id(&self) -> Option<Uuid> {
        self.operation_id
    }
    #[must_use]
    pub fn baseline(&self) -> Option<&TaskBaseline> {
        self.baseline.as_ref()
    }

    pub fn submit(&mut self) -> Result<ClientMessage, StateError> {
        if self.stage != MutationStage::Prepared {
            return Err(StateError::Busy);
        }
        self.stage = MutationStage::InFlight;
        Ok(self.message.clone())
    }

    pub fn mark_uncertain(&mut self) {
        if self.stage == MutationStage::InFlight {
            self.stage = MutationStage::Uncertain;
        }
    }

    pub fn retry(&mut self) -> Result<ClientMessage, StateError> {
        if matches!(self.kind, FormKind::TaskRequest | FormKind::WorkspaceCreate) {
            return Err(StateError::RequestNotRetryable);
        }
        if self.stage != MutationStage::Uncertain {
            return Err(StateError::NotUncertain);
        }
        self.stage = MutationStage::InFlight;
        Ok(self.message.clone())
    }

    pub fn conflict(&mut self, current_version: Option<i64>) {
        self.current_version = current_version;
        self.stage = MutationStage::Conflict;
    }

    pub fn receipt(&mut self, receipt: &TaskMutationResult) -> Result<(), StateError> {
        if self.operation_id != Some(receipt.operation_id)
            || receipt.task.summary.workspace != self.stamp.workspace.as_str()
            || self
                .baseline
                .as_ref()
                .is_some_and(|baseline| baseline.task_id != receipt.task.summary.id)
        {
            return Err(StateError::WrongReceipt);
        }
        self.stage = MutationStage::Applied;
        Ok(())
    }
}

impl std::fmt::Debug for PreparedMutation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedMutation")
            .field("stamp", &self.stamp)
            .field("kind", &self.kind)
            .field("request_id", &self.request_id)
            .field("operation_id", &self.operation_id)
            .field("stage", &self.stage)
            .field("current_version", &self.current_version)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DetailKind {
    Event(i64),
    Task,
    Member(usize),
    TaskHistory,
}

#[derive(Clone, Debug)]
pub enum Modal {
    Help {
        scroll: usize,
    },
    Detail {
        kind: DetailKind,
        scroll: usize,
    },
    Form(FormState),
    Detach {
        confirmation: InputBuffer,
        scroll: usize,
    },
    Stop {
        confirmation: InputBuffer,
    },
    /// Token-bearing prompt bytes belong to the runtime's private invite buffer,
    /// not to render state, error notices, event history, or Debug output.
    InviteReady {
        workspace: WorkspaceName,
        provider: Option<String>,
        expires_at: i64,
    },
}

#[derive(Clone, Debug)]
pub struct Notice {
    /// Runtime supplies a static, redacted description rather than a raw error.
    pub message: &'static str,
    pub error: Option<RouterErrorCode>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RenderState {
    pub no_color: bool,
    pub dirty: bool,
}

#[derive(Clone, Debug)]
pub struct UiState {
    pub header: Header,
    pub connection: Connection,
    pub workspace: Option<WorkspaceName>,
    generation: u64,
    pub switching: Option<WorkspaceSwitch>,
    pub workspaces: WorkspacePage,
    pub chat: ChatState,
    pub tasks: TaskPage,
    pub members: Vec<AgentDescriptor>,
    pub member_selected: usize,
    members_revision: u64,
    pub members_loading: bool,
    pub tab: Tab,
    pub focus: Focus,
    pub composer: Composer,
    pub modal: Option<Modal>,
    /// The draft is frozen before the asynchronous latest-detail check starts.
    pub form_busy: bool,
    /// Accepted exit intent fences queued/preflighting mutations under the shared lock.
    pub detaching: bool,
    /// A pending-request detach prompt must not replace a live mutation draft.
    pub suspended_modal: Option<Modal>,
    pub mutation: Option<PreparedMutation>,
    pub pending_requests: BTreeMap<String, PendingRequest>,
    pub notice: Option<Notice>,
    pub detail_scroll: usize,
    pub width: u16,
    pub height: u16,
    pub render: RenderState,
    reads_in_flight: usize,
}

impl UiState {
    #[must_use]
    pub fn new(header: Header, no_color: bool) -> Self {
        Self {
            header,
            connection: Connection::Connecting,
            workspace: None,
            generation: 0,
            switching: None,
            workspaces: WorkspacePage::default(),
            chat: ChatState::default(),
            tasks: TaskPage::default(),
            members: Vec::new(),
            member_selected: 0,
            members_revision: 0,
            members_loading: false,
            tab: Tab::Chat,
            focus: Focus::Main,
            composer: Composer::default(),
            modal: None,
            form_busy: false,
            detaching: false,
            suspended_modal: None,
            mutation: None,
            pending_requests: BTreeMap::new(),
            notice: None,
            detail_scroll: 0,
            width: 0,
            height: 0,
            render: RenderState {
                no_color,
                dirty: true,
            },
            reads_in_flight: 0,
        }
    }

    #[must_use]
    pub fn stamp(&self) -> Option<WorkspaceStamp> {
        self.workspace.clone().map(|workspace| WorkspaceStamp {
            generation: self.generation,
            workspace,
        })
    }
    #[must_use]
    pub fn accepts(&self, stamp: &WorkspaceStamp) -> bool {
        self.generation == stamp.generation && self.workspace.as_ref() == Some(&stamp.workspace)
    }
    /// Display read successes and failures share the same workspace/revision fence.
    #[must_use]
    pub fn accepts_history(&self, ticket: &ReadTicket) -> bool {
        self.accepts(&ticket.stamp) && ticket.revision == self.chat.history_revision
    }
    #[must_use]
    pub fn terminal_large_enough(&self) -> bool {
        self.width >= MIN_WIDTH && self.height >= MIN_HEIGHT
    }
    pub fn resize(&mut self, width: u16, height: u16) {
        self.width = width;
        self.height = height;
        self.render.dirty = true;
    }
    pub fn mark_dirty(&mut self) {
        self.render.dirty = true;
    }

    #[must_use]
    pub fn form(&self) -> Option<&FormState> {
        match &self.modal {
            Some(Modal::Form(form)) => Some(form),
            Some(Modal::Detach { .. }) => match &self.suspended_modal {
                Some(Modal::Form(form)) => Some(form),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn form_mut(&mut self) -> Option<&mut FormState> {
        match &mut self.modal {
            Some(Modal::Form(form)) => Some(form),
            Some(Modal::Detach { .. }) => match &mut self.suspended_modal {
                Some(Modal::Form(form)) => Some(form),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn close_form(&mut self) {
        if matches!(self.modal, Some(Modal::Form(_))) {
            self.modal = None;
        }
        if matches!(self.suspended_modal, Some(Modal::Form(_))) {
            self.suspended_modal = None;
        }
        self.form_busy = false;
        self.render.dirty = true;
    }

    pub fn acquire_read(&mut self) -> bool {
        if self.reads_in_flight >= MAX_READS {
            return false;
        }
        self.reads_in_flight += 1;
        true
    }
    pub fn release_read(&mut self) {
        self.reads_in_flight = self.reads_in_flight.saturating_sub(1);
    }

    /// Cancel resource authority without changing the connection or operation state.
    /// The controller separately releases slots owned by cancelled read workers.
    pub fn invalidate_reads(&mut self) {
        self.tasks.invalidate_reads();
        self.members_revision = self.members_revision.wrapping_add(1);
        self.members_loading = false;
        self.workspaces.revision = self.workspaces.revision.wrapping_add(1);
        self.workspaces.loading = false;
        self.chat.history_revision = self.chat.history_revision.wrapping_add(1);
        self.render.dirty = true;
    }

    pub fn connection_changed(&mut self, connection: Connection) -> bool {
        let refresh = matches!(connection, Connection::Connected { epoch } if self.connection != (Connection::Connected { epoch }));
        self.connection = connection;
        if !matches!(connection, Connection::Connected { .. }) {
            self.chat.synchronized = false;
            if let Some(mutation) = self.mutation.as_mut() {
                mutation.mark_uncertain();
            }
            self.composer.mark_uncertain();
            for pending in self.pending_requests.values_mut() {
                pending.stage = RequestStage::Uncertain;
            }
        } else if refresh {
            self.chat.synchronized = false;
        }
        if refresh || !matches!(connection, Connection::Connected { .. }) {
            self.invalidate_reads();
        }
        self.render.dirty = true;
        refresh
    }

    pub fn writable(&self) -> Result<(), StateError> {
        if !self.terminal_large_enough() {
            return Err(StateError::TerminalTooSmall);
        }
        if !matches!(self.connection, Connection::Connected { .. }) {
            return Err(StateError::NotConnected);
        }
        if self.workspace.is_none() {
            return Err(StateError::NoWorkspace);
        }
        if !self.chat.synchronized {
            return Err(StateError::Synchronizing);
        }
        if self.detaching
            || self.switching.is_some()
            || self.mutation.as_ref().is_some_and(|mutation| {
                matches!(
                    mutation.stage,
                    MutationStage::InFlight | MutationStage::Uncertain
                )
            })
            || self.composer.is_locked()
        {
            return Err(StateError::Busy);
        }
        Ok(())
    }

    /// Workspace creation is valid even when the server has no rooms yet.
    /// Other forms use the selected, joined workspace and normal write fence.
    pub fn prepare_form(&self, form: &FormState) -> Result<PreparedMutation, StateError> {
        if form.kind == FormKind::WorkspaceCreate {
            if !self.header.is_admin {
                return Err(StateError::PermissionDenied);
            }
            if !self.terminal_large_enough() {
                return Err(StateError::TerminalTooSmall);
            }
            if !matches!(self.connection, Connection::Connected { .. }) {
                return Err(StateError::NotConnected);
            }
            if self.detaching || self.switching.is_some() || self.mutation.is_some() {
                return Err(StateError::Busy);
            }
            let workspace = WorkspaceName::parse(form.value(FieldId::Name).to_owned())
                .map_err(|_| StateError::InvalidInput)?;
            return form.prepare(WorkspaceStamp {
                generation: self.generation,
                workspace,
            });
        }
        self.writable()?;
        if form.kind == FormKind::TaskRequest && !self.tasks.can_request() {
            return Err(StateError::StopUnconfirmed);
        }
        form.prepare(self.stamp().ok_or(StateError::NoWorkspace)?)
    }

    pub fn begin_switch(&mut self, target: WorkspaceName) -> Result<(), StateError> {
        if self.detaching
            || !self.pending_requests.is_empty()
            || self.modal.is_some()
            || self.suspended_modal.is_some()
            || self.form_busy
            || self.mutation.is_some()
            || self.composer.is_locked()
            || !self.composer.input.text().is_empty()
            || self.switching.is_some()
        {
            return Err(StateError::Busy);
        }
        if !matches!(self.connection, Connection::Connected { .. }) {
            return Err(StateError::NotConnected);
        }
        self.switching = Some(WorkspaceSwitch {
            target,
            phase: SwitchPhase::Unsubscribing,
        });
        self.render.dirty = true;
        Ok(())
    }

    /// Call only after unsubscribe -> confirmed leave -> confirmed new join.
    /// On `LeaveUnconfirmed` keep the previous selection and recover its membership
    /// using a new client; never call this method for a speculative target.
    pub fn commit_workspace(&mut self, workspace: WorkspaceName) {
        self.generation = self.generation.wrapping_add(1);
        self.switching = None;
        self.modal = None;
        self.suspended_modal = None;
        self.form_busy = false;
        self.mutation = None;
        self.composer = Composer::default();
        self.pending_requests.clear();
        self.notice = None;
        self.chat = ChatState::default();
        self.tasks = TaskPage::default();
        self.members.clear();
        self.member_selected = 0;
        self.members_revision = self.members_revision.wrapping_add(1);
        self.members_loading = false;
        self.detail_scroll = 0;
        if let Some(index) = self
            .workspaces
            .rows
            .iter()
            .position(|row| row.name == workspace)
        {
            self.workspaces.selected = index;
        }
        self.workspace = Some(workspace);
        self.render.dirty = true;
    }

    pub fn begin_members_load(&mut self) -> Option<ReadTicket> {
        if self.members_loading {
            return None;
        }
        let stamp = self.stamp()?;
        self.members_revision = self.members_revision.wrapping_add(1);
        self.members_loading = true;
        Some(ReadTicket {
            stamp,
            revision: self.members_revision,
        })
    }

    pub fn apply_members(&mut self, ticket: &ReadTicket, members: Vec<AgentDescriptor>) -> bool {
        if !self.accepts(&ticket.stamp) || ticket.revision != self.members_revision {
            return false;
        }
        self.members = members;
        self.member_selected = self
            .member_selected
            .min(self.members.len().saturating_sub(1));
        self.members_loading = false;
        self.render.dirty = true;
        true
    }

    pub fn fail_members_load(&mut self, ticket: &ReadTicket) {
        if self.accepts(&ticket.stamp) && ticket.revision == self.members_revision {
            self.members_loading = false;
        }
    }

    /// Seed only the deliberately omitted prefix on initial recent-history load.
    /// For a later End/reload this method updates display only; live progress and
    /// task versions remain independent and are advanced by `apply_event`.
    pub fn apply_recent_page(
        &mut self,
        ticket: &ReadTicket,
        page: HistoryPage,
    ) -> Result<Vec<EventApplied>, StateError> {
        if !self.accepts_history(ticket) {
            return Ok(Vec::new());
        }
        if !valid_history_page(&page, &ticket.stamp.workspace) {
            return Err(StateError::InvalidPage);
        }
        let initial = self.chat.applied_cursor.is_none();
        self.chat.mode = HistoryMode::Live;
        let previous = std::mem::replace(&mut self.chat.buffer, EventBuffer::new(MAX_LIVE_EVENTS));
        self.chat.display.next_cursor = page.next_cursor;
        self.chat.display.has_more = page.has_more;
        self.chat.new_events = 0;
        self.chat.follow = true;
        let mut applied = Vec::new();
        if initial {
            self.chat.applied_cursor = Some(
                page.events
                    .first()
                    .map_or(page.next_cursor, |event| event.seq.saturating_sub(1)),
            );
            for event in page.events {
                applied.push(self.apply_event(&ticket.stamp, event));
            }
        } else {
            for event in page.events.into_iter().take(usize::from(MAX_PAGE_LIMIT)) {
                if event.workspace != ticket.stamp.workspace {
                    return Err(StateError::InvalidPage);
                }
                self.chat.buffer.push(event);
            }
            for event in previous
                .events
                .into_iter()
                .filter(|event| event.seq > page.next_cursor)
            {
                self.chat.buffer.push(event);
            }
            self.chat.selected_seq = self.chat.buffer.events().back().map(|event| event.seq);
        }
        self.render.dirty = true;
        Ok(applied)
    }

    pub fn apply_past_page(
        &mut self,
        ticket: &ReadTicket,
        page: HistoryPage,
    ) -> Result<bool, StateError> {
        if !self.accepts_history(ticket) {
            return Ok(false);
        }
        if !valid_history_page(&page, &ticket.stamp.workspace) {
            return Err(StateError::InvalidPage);
        }
        self.chat.enter_past();
        self.chat.display.next_cursor = page.next_cursor;
        self.chat.display.has_more = page.has_more
            || self
                .chat
                .applied_cursor
                .is_some_and(|cursor| cursor > page.next_cursor);
        for event in page.events.into_iter().take(MAX_HISTORY_EVENTS) {
            if event.workspace != ticket.stamp.workspace {
                return Err(StateError::InvalidPage);
            }
            self.chat.buffer.push(event);
        }
        self.chat.selected_seq = self.chat.buffer.events().front().map(|event| event.seq);
        self.render.dirty = true;
        Ok(true)
    }

    /// Consume an assembled previous page without touching live ACK progress.
    pub fn apply_previous_page(
        &mut self,
        ticket: &ReadTicket,
        load: PreviousHistoryLoad,
    ) -> Result<bool, StateError> {
        if !self.accepts_history(ticket) || self.chat.mode != HistoryMode::Past {
            return Ok(false);
        }
        if !load.done
            || load.boundary <= 0
            || load.buffer.events().iter().any(|event| {
                event.workspace != ticket.stamp.workspace
                    || event.seq <= 0
                    || event.seq >= load.boundary
            })
            || load
                .buffer
                .events()
                .iter()
                .zip(load.buffer.events().iter().skip(1))
                .any(|(left, right)| left.seq.checked_add(1) != Some(right.seq))
        {
            return Err(StateError::InvalidPage);
        }
        self.chat.display.next_cursor = load.buffer.events().back().map_or(0, |event| event.seq);
        self.chat.display.has_more = self
            .chat
            .applied_cursor
            .is_some_and(|cursor| cursor > self.chat.display.next_cursor);
        self.chat.buffer = load.buffer;
        self.chat.selected_seq = self
            .chat
            .buffer
            .events()
            .iter()
            .rev()
            .find(|event| self.chat.visible(event))
            .map(|event| event.seq);
        self.chat.follow = false;
        self.render.dirty = true;
        Ok(true)
    }

    pub fn apply_event(&mut self, stamp: &WorkspaceStamp, event: WorkspaceEvent) -> EventApplied {
        let mut effect = EventApplied {
            disposition: EventDisposition::StaleWorkspace,
            ack: None,
            refresh_task_detail: false,
            request_resolved: false,
        };
        if !self.accepts(stamp) || event.workspace != stamp.workspace {
            return effect;
        }
        if event.seq <= 0 {
            effect.disposition = EventDisposition::Invalid;
            return effect;
        }
        let Some(cursor) = self.chat.applied_cursor else {
            effect.disposition = EventDisposition::Gap {
                expected: 0,
                received: event.seq,
            };
            return effect;
        };
        if event.seq <= cursor {
            effect.disposition = EventDisposition::Duplicate;
            effect.ack = Some(event.seq);
            return effect;
        }
        if event.seq != cursor.saturating_add(1) {
            self.chat.synchronized = false;
            self.render.dirty = true;
            effect.disposition = EventDisposition::Gap {
                expected: cursor.saturating_add(1),
                received: event.seq,
            };
            return effect;
        }
        if event.kind == WorkspaceEventKind::Task {
            if let Some(task_event) = event
                .content
                .as_deref()
                .and_then(|content| serde_json::from_str::<TaskEvent>(content).ok())
                .filter(|task_event| {
                    task_event.task.workspace == stamp.workspace.as_str()
                        && Some(task_event.task.id) == event.task_id
                })
            {
                if task_event.change == crate::tasks::TaskChange::Begun {
                    for pending in self.pending_requests.values_mut().filter(|pending| {
                        pending.task_id == task_event.task.id
                            && pending.expected_version < task_event.task.version
                    }) {
                        pending.stage = RequestStage::ExecutionObserved;
                    }
                }
                effect.refresh_task_detail = self.tasks.observe(task_event.task);
            } else {
                effect.disposition = EventDisposition::Invalid;
                self.chat.synchronized = false;
                self.render.dirty = true;
                return effect;
            }
        }
        if let Some(request_id) = event.request_id.as_deref() {
            if event.kind == WorkspaceEventKind::Result {
                effect.request_resolved = self.pending_requests.remove(request_id).is_some();
            }
            if event.kind == WorkspaceEventKind::Chat
                && self.composer.pending().is_some_and(|post| {
                    post.request_id == request_id
                        && event.content.as_deref() == Some(post.content.as_str())
                })
            {
                self.composer.acknowledge(request_id);
            }
        }
        self.chat.applied_cursor = Some(event.seq);
        effect.disposition = EventDisposition::Applied;
        effect.ack = Some(event.seq);
        if self.chat.mode == HistoryMode::Live {
            if self.chat.follow {
                self.chat.selected_seq = Some(event.seq);
            } else {
                self.chat.new_events = self.chat.new_events.saturating_add(1);
            }
            // A recent display refresh can include not-yet-applied live events.
            if !self
                .chat
                .buffer
                .events()
                .iter()
                .any(|old| old.seq == event.seq)
            {
                self.chat.buffer.push(event);
            }
        } else {
            self.chat.new_events = self.chat.new_events.saturating_add(1);
            self.chat.display.has_more |= event.seq > self.chat.display.next_cursor;
        }
        self.render.dirty = true;
        effect
    }

    pub fn subscription_live(&mut self, stamp: &WorkspaceStamp, next_cursor: i64) -> bool {
        if !self.accepts(stamp) || self.chat.applied_cursor != Some(next_cursor) {
            return false;
        }
        self.chat.synchronized = true;
        self.render.dirty = true;
        true
    }

    pub fn apply_task_page(
        &mut self,
        ticket: &ReadTicket,
        tasks: Vec<TaskSummary>,
        next_cursor: i64,
        has_more: bool,
    ) -> bool {
        if !self.accepts(&ticket.stamp)
            || tasks
                .iter()
                .any(|task| task.workspace != ticket.stamp.workspace.as_str())
        {
            return false;
        }
        let applied = self
            .tasks
            .apply_page(ticket.revision, tasks, next_cursor, has_more);
        self.render.dirty |= applied;
        applied
    }

    pub fn apply_task_detail(&mut self, ticket: &ReadTicket, task: TaskDetail) -> bool {
        if !self.accepts(&ticket.stamp) || task.summary.workspace != ticket.stamp.workspace.as_str()
        {
            return false;
        }
        let applied = self.tasks.apply_detail(ticket.revision, task);
        self.render.dirty |= applied;
        applied
    }

    pub fn apply_task_history(&mut self, ticket: &ReadTicket, page: TaskHistoryPage) -> bool {
        if !self.accepts(&ticket.stamp) || page.workspace != ticket.stamp.workspace {
            return false;
        }
        let applied = self.tasks.apply_history(ticket.revision, page);
        self.render.dirty |= applied;
        applied
    }

    pub fn apply_receipt(
        &mut self,
        stamp: &WorkspaceStamp,
        receipt: TaskMutationResult,
    ) -> Result<bool, StateError> {
        if !self.accepts(stamp) {
            return Ok(false);
        }
        let mutation = self.mutation.as_mut().ok_or(StateError::WrongReceipt)?;
        mutation.receipt(&receipt)?;
        if mutation.kind == FormKind::TaskCreate {
            self.tasks.select(Some(receipt.task.summary.id));
            self.tab = Tab::Tasks;
        }
        self.tasks.observe(receipt.task.summary.clone());
        if self.tasks.selected_id == Some(receipt.task.summary.id) {
            self.tasks
                .apply_detail(self.tasks.detail_revision, receipt.task);
        }
        self.render.dirty = true;
        Ok(true)
    }

    /// Track a `TaskRequest` before sending, so a fast result cannot race its entry.
    pub fn track_request(&mut self, mutation: &PreparedMutation) -> Result<(), StateError> {
        self.writable()?;
        if !self.accepts(&mutation.stamp) || mutation.kind != FormKind::TaskRequest {
            return Err(StateError::InvalidInput);
        }
        if self.pending_requests.len() >= MAX_PENDING_REQUESTS {
            return Err(StateError::Busy);
        }
        let baseline = mutation.baseline().ok_or(StateError::MissingTask)?;
        if self
            .pending_requests
            .values()
            .any(|pending| pending.task_id == baseline.task_id)
        {
            return Err(StateError::Busy);
        }
        self.pending_requests.insert(
            mutation.request_id.clone(),
            PendingRequest {
                request_id: mutation.request_id.clone(),
                task_id: baseline.task_id,
                expected_version: baseline.version,
                stage: RequestStage::Sending,
            },
        );
        self.render.dirty = true;
        Ok(())
    }

    pub fn request_accepted(&mut self, request_id: &str) {
        if let Some(request) = self.pending_requests.get_mut(request_id) {
            if request.stage != RequestStage::ExecutionObserved {
                request.stage = RequestStage::Accepted;
            }
            self.render.dirty = true;
        }
    }

    /// `SendResult` resolves request lifetime, not task completion or task state.
    pub fn resolve_request(&mut self, stamp: &WorkspaceStamp, request_id: &str) -> bool {
        if !self.accepts(stamp) {
            return false;
        }
        let removed = self.pending_requests.remove(request_id).is_some();
        self.render.dirty |= removed;
        removed
    }

    #[must_use]
    pub fn can_invite(&self) -> bool {
        self.header.is_admin
            && self.header.ownership != Ownership::Remote
            && self.workspace.is_some()
    }
    #[must_use]
    pub fn can_stop(&self) -> bool {
        self.header.is_admin
            && self.header.ownership != Ownership::Remote
            && self.header.instance_id.is_some()
    }
    #[must_use]
    pub fn detach_needs_confirmation(&self) -> bool {
        !self.pending_requests.is_empty()
    }
    #[must_use]
    pub fn detach_confirmed(&self) -> bool {
        matches!(&self.modal, Some(Modal::Detach { confirmation, .. }) if confirmation.text() == "DETACH")
    }
    /// Runtime must additionally revalidate the owned instance before stopping.
    #[must_use]
    pub fn stop_confirmed(&self) -> bool {
        self.can_stop()
            && matches!(&self.modal, Some(Modal::Stop { confirmation }) if confirmation.text() == "STOP")
    }
}
