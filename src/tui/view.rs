//! Pure ratatui rendering. No terminal lifecycle, network calls, filesystem reads,
//! subprocesses, ANSI interpretation, or token-bearing prompt storage live here.

use std::borrow::Cow;

use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Cell, Clear, List, ListItem, ListState, Paragraph, Row, Table, TableState,
    },
};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use crate::{
    cli::escape_terminal,
    protocol::{AgentDescriptor, RouterErrorCode, WorkspaceEvent, WorkspaceEventKind},
    tasks::{ReportBody, TaskAttempt, TaskDetail, TaskReportRecord, TaskSummary},
};

use super::state::{
    Connection, DetailKind, Focus, FormKind, FormState, HistoryMode, InputBuffer, MIN_HEIGHT,
    MIN_WIDTH, Modal, MutationStage, Ownership, RequestStage, StateError, Tab, UiState, WIDE_WIDTH,
};

#[derive(Clone, Copy)]
struct Theme {
    no_color: bool,
}

impl Theme {
    fn color(self, color: Color) -> Style {
        if self.no_color {
            Style::default()
        } else {
            Style::default().fg(color)
        }
    }
    fn heading(self) -> Style {
        self.color(Color::Cyan).add_modifier(Modifier::BOLD)
    }
    fn muted(self) -> Style {
        self.color(Color::DarkGray)
    }
    fn warning(self) -> Style {
        self.color(Color::Yellow).add_modifier(Modifier::BOLD)
    }
    fn selected(self) -> Style {
        self.color(Color::Cyan)
            .add_modifier(Modifier::BOLD | Modifier::REVERSED)
    }
    fn border(self, focused: bool) -> Style {
        if focused {
            self.heading()
        } else {
            self.muted()
        }
    }
}

/// Runtime calls this only when dirty, at most 20 fps, and clears its dirty flag
/// after a successful terminal draw. Mutation gating also uses `UiState::resize`.
pub fn render(frame: &mut Frame<'_>, state: &UiState) {
    let area = frame.area();
    let theme = Theme {
        no_color: state.render.no_color,
    };
    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        frame.render_widget(
            Paragraph::new(vec![
                Line::styled("ASR console — terminal too small", theme.heading()),
                Line::from(format!(
                    "Resize to at least {MIN_WIDTH}×{MIN_HEIGHT} (currently {}×{}).",
                    area.width, area.height
                )),
                Line::from("Connection retained. Mutations are disabled."),
                Line::from("Leaving this screen does not stop the router."),
            ])
            .block(Block::default().borders(Borders::ALL)),
            area,
        );
        return;
    }
    let regions = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(1),
        Constraint::Length(2),
    ])
    .split(area);
    render_header(frame, regions[0], state, theme);
    let wide = area.width >= WIDE_WIDTH;
    let columns = if wide {
        Layout::horizontal([
            Constraint::Length(24),
            Constraint::Min(1),
            Constraint::Length(34),
        ])
        .split(regions[1])
    } else {
        Layout::horizontal([Constraint::Length(20), Constraint::Min(1)]).split(regions[1])
    };
    render_workspaces(frame, columns[0], state, theme);
    let main = Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).split(columns[1]);
    render_tabs(frame, main[0], state, theme);
    match state.tab {
        Tab::Chat => render_chat(frame, main[1], state, theme),
        Tab::Tasks => render_tasks(frame, main[1], state, theme),
        Tab::Members => render_members(frame, main[1], state, theme),
    }
    if wide {
        let kind = match state.tab {
            Tab::Chat => state.chat.selected_seq.map(DetailKind::Event),
            Tab::Tasks => Some(DetailKind::Task),
            Tab::Members => Some(DetailKind::Member(state.member_selected)),
        };
        render_detail(frame, columns[2], state, kind, state.detail_scroll, theme);
    }
    render_footer(frame, regions[2], state, theme);
    if let Some(modal) = &state.modal {
        render_modal(frame, area, state, modal, theme);
    }
}

fn pane(title: impl Into<Line<'static>>, focused: bool, theme: Theme) -> Block<'static> {
    Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(theme.border(focused))
}

fn render_header(frame: &mut Frame<'_>, area: Rect, state: &UiState, theme: Theme) {
    let connection = match state.connection {
        Connection::Connecting => "CONNECTING",
        Connection::Connected { .. } => "CONNECTED",
        Connection::Reconnecting => "RECONNECTING · writes disabled",
        Connection::Closed { .. } => "CLOSED · writes disabled",
    };
    let ownership = match state.header.ownership {
        Ownership::Owned => "owned",
        Ownership::Reused => "reused",
        Ownership::Remote => "remote",
    };
    let identity = state
        .header
        .instance_id
        .filter(|_| state.header.ownership != Ownership::Remote)
        .map_or_else(
            || ownership.to_owned(),
            |instance| format!("{ownership} · instance {instance}"),
        );
    let endpoint = display_endpoint(&state.header.endpoint);
    let sync = if state.workspace.is_some() && !state.chat.synchronized {
        " · SYNCHRONIZING"
    } else {
        ""
    };
    let title = Line::from(vec![
        Span::styled(" ASR  ", theme.heading()),
        Span::raw(escape_terminal(
            state.header.profile.as_deref().unwrap_or("local"),
        )),
        Span::styled(format!("  {connection}{sync}  "), theme.heading()),
        Span::raw(format!(
            "{}  {endpoint}",
            escape_terminal(&state.header.transport)
        )),
    ]);
    frame.render_widget(
        Paragraph::new(vec![
            title,
            Line::from(format!(
                " {identity} · router stays running · reattach: asr ui"
            )),
            Line::styled(
                " q/Ctrl-C: leave (pending-request check) · Ctrl-X: explicit owned-router stop",
                theme.muted(),
            ),
        ]),
        area,
    );
}

/// Even an accidentally unvalidated endpoint cannot expose URL credentials,
/// query tokens or fragments in the header or stop confirmation.
#[must_use]
pub fn display_endpoint(endpoint: &str) -> String {
    let Ok(mut url) = url::Url::parse(endpoint) else {
        return "[invalid endpoint]".to_owned();
    };
    if !matches!(url.scheme(), "ws" | "wss") {
        return "[invalid endpoint]".to_owned();
    }
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    // Router endpoints have a fixed path; do not render arbitrary internal paths.
    url.set_path("/ws");
    escape_terminal(url.as_str())
}

fn render_workspaces(frame: &mut Frame<'_>, area: Rect, state: &UiState, theme: Theme) {
    let block = pane(" Workspaces ", state.focus == Focus::Workspaces, theme);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let sections = Layout::vertical([Constraint::Min(1), Constraint::Length(4)]).split(inner);
    if state.workspaces.rows.is_empty() {
        let message = if state.workspaces.loading {
            "Loading…"
        } else if state.header.is_admin {
            "No workspaces.\nCtrl-N creates one."
        } else {
            "No accessible rooms.\nAsk the server admin."
        };
        frame.render_widget(Paragraph::new(message), sections[0]);
    } else {
        let width = usize::from(sections[0].width.saturating_sub(7));
        let rows = state
            .workspaces
            .rows
            .iter()
            .map(|workspace| {
                let joined = if state.workspace.as_ref() == Some(&workspace.name) {
                    "*"
                } else {
                    " "
                };
                ListItem::new(format!(
                    "{joined} {}  {}",
                    preview(workspace.name.as_str(), width),
                    workspace.connected_agents
                ))
            })
            .collect::<Vec<_>>();
        let mut selection = ListState::default().with_selected(Some(state.workspaces.selected));
        frame.render_stateful_widget(
            List::new(rows)
                .highlight_symbol("> ")
                .highlight_style(theme.selected()),
            sections[0],
            &mut selection,
        );
    }
    let pages = &state.workspaces.pagination;
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(format!(
                "Page {} · more: {}",
                pages.page_number,
                yes_no(pages.has_more)
            )),
            Line::styled("[ / ] previous / next", theme.muted()),
            Line::from(if state.header.is_admin {
                "Ctrl-N: create"
            } else {
                "Create: admin only"
            }),
            Line::from(if state.can_invite() {
                "F4: invite client"
            } else {
                "Invite: owned admin"
            }),
        ]),
        sections[1],
    );
}

fn render_tabs(frame: &mut Frame<'_>, area: Rect, state: &UiState, theme: Theme) {
    let tabs = [
        (Tab::Chat, " F1 Chat "),
        (Tab::Tasks, " F2 Tasks "),
        (Tab::Members, " F3 Members "),
    ];
    let spans = tabs
        .into_iter()
        .map(|(tab, title)| {
            Span::styled(
                title,
                if tab == state.tab {
                    theme.selected()
                } else {
                    theme.muted()
                },
            )
        })
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn render_chat(frame: &mut Frame<'_>, area: Rect, state: &UiState, theme: Theme) {
    let regions = Layout::vertical([Constraint::Min(1), Constraint::Length(5)]).split(area);
    let mode = if state.chat.mode == HistoryMode::Past {
        "history page"
    } else if state.chat.follow {
        "live tail"
    } else {
        "paused scroll"
    };
    let mut title = format!(
        " Chat · {mode} · {} ",
        if state.chat.all_events {
            "all events"
        } else {
            "chat / work"
        }
    );
    if state.chat.new_events > 0 {
        title = format!(" Chat · +{} new (End) · {mode} ", state.chat.new_events);
    }
    if state.chat.mode == HistoryMode::Past
        && state
            .chat
            .buffer
            .events()
            .front()
            .is_some_and(|event| event.seq == 1)
    {
        title.push_str("· start ");
    }
    if state.chat.buffer.truncated {
        title.push_str("· older history available ");
    }
    let block = pane(title, state.focus == Focus::Main, theme);
    let inner = block.inner(regions[0]);
    frame.render_widget(block, regions[0]);
    let events = state.chat.buffer.events();
    let count = events
        .iter()
        .filter(|event| state.chat.visible(event))
        .count();
    if count == 0 {
        frame.render_widget(
            Paragraph::new(if state.workspace.is_none() {
                "Select a workspace to see its history."
            } else {
                "No events on this page."
            }),
            inner,
        );
    } else {
        // Build only screen-sized previews, not 2,000 full-content ListItems.
        let selected = events
            .iter()
            .filter(|event| state.chat.visible(event))
            .position(|event| Some(event.seq) == state.chat.selected_seq)
            .unwrap_or(if state.chat.follow { count - 1 } else { 0 });
        let rows = usize::from(inner.height / 3).max(1);
        let start = if state.chat.follow {
            count.saturating_sub(rows)
        } else {
            selected
                .saturating_sub(rows / 2)
                .min(count.saturating_sub(rows))
        };
        let items = events
            .iter()
            .filter(|event| state.chat.visible(event))
            .skip(start)
            .take(rows)
            .map(|event| {
                let header = format!(
                    "{} #{} {} [{}]",
                    utc_clock(event.created_at),
                    event.seq,
                    escape_terminal(&event.actor_id),
                    event_kind(event.kind)
                );
                let route = event_route(event);
                ListItem::new(vec![
                    Line::styled(
                        preview(&header, usize::from(inner.width.saturating_sub(2))),
                        theme.heading(),
                    ),
                    Line::styled(
                        preview(&route, usize::from(inner.width.saturating_sub(2))),
                        theme.muted(),
                    ),
                    Line::from(event_preview(
                        event,
                        usize::from(inner.width.saturating_sub(2)),
                    )),
                ])
            })
            .collect::<Vec<_>>();
        let mut selection =
            ListState::default().with_selected(Some(selected.saturating_sub(start)));
        frame.render_stateful_widget(
            List::new(items)
                .highlight_symbol("> ")
                .highlight_style(theme.selected()),
            inner,
            &mut selection,
        );
    }
    let pending = state.composer.pending();
    let label = if pending.is_some_and(|post| post.uncertain) {
        " Chat draft · uncertain; Ctrl-S retries the same message "
    } else if pending.is_some() {
        " Chat draft · awaiting acknowledgement "
    } else if state.form_busy {
        " Chat draft · preparing; input locked "
    } else {
        " Message · Ctrl-S posts; does NOT run a model "
    };
    let block = pane(label, state.focus == Focus::Composer, theme);
    let inner = block.inner(regions[1]);
    frame.render_widget(block, regions[1]);
    render_input(
        frame,
        inner,
        &state.composer.input,
        state.focus == Focus::Composer
            && state.modal.is_none()
            && !state.form_busy
            && !state.composer.is_locked(),
        theme,
    );
}

fn render_tasks(frame: &mut Frame<'_>, area: Rect, state: &UiState, theme: Theme) {
    let sections = Layout::vertical([Constraint::Min(1), Constraint::Length(3)]).split(area);
    let block = pane(
        " Tasks · assignment ≠ execution ≠ completion ",
        state.focus == Focus::Main,
        theme,
    );
    let inner = block.inner(sections[0]);
    frame.render_widget(block, sections[0]);
    let selected = state
        .tasks
        .rows
        .keys()
        .position(|id| Some(*id) == state.tasks.selected_id);
    if state.tasks.rows.is_empty() {
        frame.render_widget(Paragraph::new("No tasks match this page/filter.\nCtrl-T creates a task; assignment and request are separate."), inner);
    } else if inner.width >= 96 {
        let rows = state
            .tasks
            .rows
            .values()
            .map(|task| {
                Row::new(vec![
                    Cell::from(task.id.to_string()),
                    Cell::from(escape_terminal(&task.title)),
                    Cell::from(task.state.as_str()),
                    Cell::from(optional_text(task.assigned_agent_id.as_deref())),
                    Cell::from(optional_text(task.last_executor_id.as_deref())),
                    Cell::from(task.version.to_string()),
                    Cell::from(task.stop_evidence.map_or("—", |evidence| evidence.as_str())),
                    Cell::from(
                        task.last_checkpoint_at
                            .map_or_else(|| "—".to_owned(), utc_clock),
                    ),
                ])
            })
            .collect::<Vec<_>>();
        let table = Table::new(
            rows,
            [
                Constraint::Length(5),
                Constraint::Min(12),
                Constraint::Length(11),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(5),
                Constraint::Length(9),
                Constraint::Length(12),
            ],
        )
        .header(
            Row::new([
                "ID",
                "Title",
                "State",
                "Assignee",
                "Executor",
                "Ver",
                "Stop",
                "Checkpoint",
            ])
            .style(theme.heading()),
        )
        .row_highlight_style(theme.selected())
        .highlight_symbol("> ");
        let mut selection = TableState::default().with_selected(selected);
        frame.render_stateful_widget(table, inner, &mut selection);
    } else {
        let visible = usize::from(inner.height / 5).max(1);
        let start = selected
            .unwrap_or(0)
            .saturating_sub(visible / 2)
            .min(state.tasks.rows.len().saturating_sub(visible));
        let width = usize::from(inner.width.saturating_sub(2));
        let items = state
            .tasks
            .rows
            .values()
            .skip(start)
            .take(visible)
            .map(|task| {
                ListItem::new(vec![
                    Line::styled(
                        preview(&format!("#{} {}", task.id, task.title), width),
                        theme.heading(),
                    ),
                    Line::from(format!(
                        "{} · v{} · stop: {}",
                        task.state.as_str(),
                        task.version,
                        task.stop_evidence.map_or("—", |evidence| evidence.as_str())
                    )),
                    Line::from(preview(
                        &format!(
                            "Assignee: {}",
                            task.assigned_agent_id.as_deref().unwrap_or("unassigned")
                        ),
                        width,
                    )),
                    Line::from(preview(
                        &format!(
                            "Executor: {}",
                            task.last_executor_id.as_deref().unwrap_or("none")
                        ),
                        width,
                    )),
                    Line::styled(
                        format!(
                            "Checkpoint: {}",
                            task.last_checkpoint_at
                                .map_or_else(|| "none".to_owned(), utc_timestamp)
                        ),
                        theme.muted(),
                    ),
                ])
            })
            .collect::<Vec<_>>();
        let mut selection =
            ListState::default().with_selected(selected.map(|index| index.saturating_sub(start)));
        frame.render_stateful_widget(
            List::new(items)
                .highlight_symbol("> ")
                .highlight_style(theme.selected()),
            inner,
            &mut selection,
        );
    }
    let states = state
        .tasks
        .filter
        .states
        .iter()
        .map(|state| state.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let filter = format!(
        "Filter: {states} · assignee: {}",
        state
            .tasks
            .filter
            .assigned_agent_id
            .as_deref()
            .unwrap_or("all")
    );
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(preview(&filter, usize::from(sections[1].width))),
            Line::from(format!(
                "Page {} · hasMore: {} · [ / ] pages · / filters",
                state.tasks.pagination.page_number,
                yes_no(state.tasks.pagination.has_more)
            )),
            Line::styled(
                if state.tasks.can_request() {
                    "a assign · r request · i interrupt · e edit · m note · c cancel · o reopen"
                } else {
                    "a assign · r blocked until authoritative task/stop fence permits"
                },
                theme.muted(),
            ),
        ]),
        sections[1],
    );
}

fn render_members(frame: &mut Frame<'_>, area: Rect, state: &UiState, theme: Theme) {
    let regions = Layout::vertical([Constraint::Min(1), Constraint::Length(2)]).split(area);
    let block = pane(" Connected agents ", state.focus == Focus::Main, theme);
    let inner = block.inner(regions[0]);
    frame.render_widget(block, regions[0]);
    if state.members.is_empty() {
        frame.render_widget(
            Paragraph::new(if state.members_loading {
                "Loading connected members…"
            } else {
                "No agents are connected to this workspace."
            }),
            inner,
        );
    } else {
        let visible = usize::from(inner.height / 2).max(1);
        let start = state
            .member_selected
            .saturating_sub(visible / 2)
            .min(state.members.len().saturating_sub(visible));
        let rows = state
            .members
            .iter()
            .skip(start)
            .take(visible)
            .map(|member| {
                ListItem::new(vec![
                    Line::styled(
                        preview(&member.agent_id, usize::from(inner.width.saturating_sub(2))),
                        theme.heading(),
                    ),
                    Line::from(format!(
                        "{:?}/{:?} · {:?} · ready: {}",
                        member.side,
                        member.client,
                        member.status,
                        yes_no(member.ready)
                    )),
                ])
            })
            .collect::<Vec<_>>();
        let mut selection =
            ListState::default().with_selected(Some(state.member_selected.saturating_sub(start)));
        frame.render_stateful_widget(
            List::new(rows)
                .highlight_symbol("> ")
                .highlight_style(theme.selected()),
            inner,
            &mut selection,
        );
    }
    frame.render_widget(
        Paragraph::new(vec![
            Line::styled("Offline identities are not listed.", theme.muted()),
            Line::styled(
                "Operator connections are not counted as agents.",
                theme.muted(),
            ),
        ]),
        regions[1],
    );
}

fn render_detail(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &UiState,
    kind: Option<DetailKind>,
    scroll: usize,
    theme: Theme,
) {
    let block = pane(
        " Detail · ↑/↓ scroll · Enter expands ",
        state.focus == Focus::Detail,
        theme,
    );
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let mut window = TextWindow::new(inner.width, inner.height, scroll);
    match kind {
        Some(DetailKind::Event(seq)) => {
            if let Some(event) = state
                .chat
                .buffer
                .events()
                .iter()
                .find(|event| event.seq == seq)
            {
                event_detail(&mut window, event);
            } else {
                window.push(
                    "This event is outside the bounded visible page. Reload its history page.",
                );
            }
        }
        Some(DetailKind::Task) => task_detail(&mut window, state),
        Some(DetailKind::Member(index)) => {
            if let Some(member) = state.members.get(index) {
                member_detail(&mut window, member);
            } else {
                window.push("Select a connected member.");
            }
        }
        Some(DetailKind::TaskHistory) => task_history(&mut window, state),
        None => window.push("Select an item, then Enter for full details."),
    }
    frame.render_widget(Paragraph::new(window.lines), inner);
}

fn event_detail(window: &mut TextWindow, event: &WorkspaceEvent) {
    window.push(&format!(
        "{} · seq {}",
        utc_timestamp(event.created_at),
        event.seq
    ));
    window.push(&format!("Kind: {}", event_kind(event.kind)));
    window.field("Actor", &event.actor_id);
    window.field("Workspace", event.workspace.as_str());
    if let Some(target) = &event.target_id {
        window.field("Target", target);
    }
    if let Some(request) = &event.request_id {
        window.field("Request", request);
    }
    if let Some(task) = event.task_id {
        window.push(&format!("Task: #{task}"));
    }
    if let Some(ok) = event.ok {
        window.push(if ok {
            "Result: OK (not task completion)"
        } else {
            "Result: ERROR"
        });
    }
    if let Some(error) = event.error {
        window.field("Error", error.as_str());
    }
    window.push("");
    window.push(event.content.as_deref().unwrap_or("No content."));
}

fn summary_detail(window: &mut TextWindow, task: &TaskSummary) {
    window.push(&format!("Task #{} · v{}", task.id, task.version));
    window.field("Title", &task.title);
    window.field("State", task.state.as_str());
    window.field(
        "Assignee",
        task.assigned_agent_id.as_deref().unwrap_or("unassigned"),
    );
    window.field(
        "Last executor",
        task.last_executor_id.as_deref().unwrap_or("none"),
    );
    window.field(
        "Stop evidence",
        task.stop_evidence
            .map_or("none", |evidence| evidence.as_str()),
    );
    if task.stop_evidence == Some(crate::tasks::StopEvidence::Unknown) {
        window.push("STOP UNCONFIRMED · new execution blocked");
    }
    if let Some(reason) = task.pause_reason {
        window.field("Pause reason", reason.as_str());
    }
    window.field("Created", &utc_timestamp(task.created_at));
    window.field("Updated", &utc_timestamp(task.updated_at));
    if let Some(at) = task.last_checkpoint_at {
        window.field("Last checkpoint", &utc_timestamp(at));
    }
}

fn task_detail(window: &mut TextWindow, state: &UiState) {
    let Some(summary) = state.tasks.selected_summary.as_ref() else {
        window.push("Select a task. Ctrl-T creates one.");
        return;
    };
    summary_detail(window, summary);
    if state.tasks.detail_stale {
        window.push("Refreshing detail: newer authoritative version received.");
        window.push("Stale description/attempt data is not used for actions.");
        return;
    }
    let Some(detail) = state.tasks.detail.as_ref() else {
        return;
    };
    full_task_detail(window, detail);
    window.push("");
    window.push("a: assign only; r: request execution");
    if state.tasks.confirmable_attempt().is_some() {
        window.push("f: confirm stopped (requires observed evidence + note)");
    }
    window.push("h: forward task history pages");
    for pending in state
        .pending_requests
        .values()
        .filter(|pending| pending.task_id == summary.id)
    {
        window.push("");
        window.field("UI request", &pending.request_id);
        window.push(request_stage(pending.stage));
    }
}

fn full_task_detail(window: &mut TextWindow, detail: &TaskDetail) {
    window.push("");
    window.field("Description", &detail.description);
    window.field("Created by", &detail.created_by);
    window.field("Updated by", &detail.updated_by);
    if let Some(attempt) = &detail.current_attempt {
        attempt_detail(window, "Current attempt", attempt);
    }
    if let Some(attempt) = &detail.last_attempt {
        attempt_detail(window, "Last attempt", attempt);
    }
    if let Some(report) = &detail.checkpoint {
        report_detail(window, "Checkpoint", report);
    }
    if let Some(report) = &detail.result {
        report_detail(window, "Result", report);
    }
    for link in &detail.links {
        window.push("");
        window.push(&format!(
            "Link: {} {} / {}",
            link.provider.as_str(),
            link.namespace,
            link.external_id
        ));
        window.field("URL", &link.url);
        window.field("Linked", &utc_timestamp(link.linked_at));
    }
    for operation in &detail.external_operations {
        window.push("");
        window.push(&format!("External operation {}", operation.id));
        window.push(&format!(
            "{} · {:?} · {:?}",
            operation.provider.as_str(),
            operation.kind,
            operation.status
        ));
        if let Some(version) = operation.source_version {
            window.push(&format!("Source version: {version}"));
        }
        if let Some(id) = &operation.external_id {
            window.field("External ID", id);
        }
        if let Some(url) = &operation.url {
            window.field("URL", url);
        }
        // An upstream error can include internal paths or credentials. Show the
        // typed status and existence of an error, not its raw diagnostic string.
        if operation.error.is_some() {
            window.push("External operation reported an error.");
        }
        window.field("Created", &utc_timestamp(operation.created_at));
        window.field("Updated", &utc_timestamp(operation.updated_at));
    }
}

fn attempt_detail(window: &mut TextWindow, label: &str, attempt: &TaskAttempt) {
    window.push("");
    window.push(label);
    window.push(&attempt.id.to_string());
    window.field("Executor", &attempt.agent_id);
    window.field("Session", &attempt.session_id.to_string());
    window.field("Work request", &attempt.work_request_id);
    window.field("Status", attempt.status.as_str());
    window.field("Stop evidence", attempt.stop_evidence.as_str());
    if let Some(reason) = attempt.reason {
        window.field("Reason", reason.as_str());
    }
    if let Some(checkpoint) = attempt.resumed_from_checkpoint_id {
        window.field("Resumed checkpoint", &checkpoint.to_string());
    }
    window.field("Started", &utc_timestamp(attempt.started_at));
    if let Some(at) = attempt.ended_at {
        window.field("Ended", &utc_timestamp(at));
    }
    if let Some(at) = attempt.stopped_at {
        window.field("Stopped", &utc_timestamp(at));
    }
}

fn report_detail(window: &mut TextWindow, label: &str, report: &TaskReportRecord) {
    window.push("");
    window.push(&format!("{label} · {}", report.id));
    window.field("Actor", &report.actor_id);
    window.field("At", &utc_timestamp(report.created_at));
    match &report.body {
        ReportBody::Text(text) => window.push(text),
        ReportBody::Checkpoint(checkpoint) => {
            window.field("Summary", &checkpoint.summary);
            window.field("Next steps", &checkpoint.next_steps);
            window.push("Artifacts:");
            for artifact in &checkpoint.artifacts {
                window.push(artifact);
            }
            window.field("Risks", &checkpoint.risks);
        }
    }
}

fn member_detail(window: &mut TextWindow, member: &AgentDescriptor) {
    window.field("Identity", &member.agent_id);
    window.push(&format!("Provider side: {:?}", member.side));
    window.push(&format!("Client: {:?}", member.client));
    window.push(&format!("Status: {:?}", member.status));
    window.field("Ready", yes_no(member.ready));
    window.push(&format!("Delivery: {:?}", member.delivery_mode));
    window.field("Session", &member.session_id.to_string());
    if let Some(activity) = &member.activity {
        window.field("Activity", activity);
    }
    window.push("Offline identities are absent; operators do not count as members.");
}

fn task_history(window: &mut TextWindow, state: &UiState) {
    let Some(page) = &state.tasks.history else {
        window.push("Task history has not been loaded.");
        return;
    };
    window.push(&format!(
        "Task #{} · forward history page {}",
        page.task_id, state.tasks.history_pagination.page_number
    ));
    window.push(&format!(
        "after: {} · next: {} · hasMore: {}",
        state.tasks.history_pagination.after.unwrap_or(0),
        page.next_cursor,
        yes_no(page.has_more)
    ));
    window.push("This page is not necessarily the latest history. [ / ] changes pages.");
    for event in &page.events {
        if window.full() {
            break;
        }
        window.push("");
        window.push(&format!(
            "seq {} · {} · {:?}",
            event.seq,
            utc_timestamp(event.created_at),
            event.event.change
        ));
        window.field("Actor", &event.actor_id);
        window.push(&format!(
            "Recorded task version {} · {}",
            event.event.task.version,
            event.event.task.state.as_str()
        ));
        if let Some(attempt) = &event.attempt {
            attempt_detail(window, "Attempt", attempt);
        }
        if let Some(report) = &event.report {
            report_detail(window, "Report", report);
        }
    }
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, state: &UiState, theme: Theme) {
    let status = if let Some(notice) = &state.notice {
        let error = notice
            .error
            .map_or_else(String::new, |error| format!(" · {}", error.as_str()));
        format!("{}{error}", notice.message)
    } else if let Some(switching) = &state.switching {
        format!(
            "Workspace transition: {:?} · target {} · writes disabled",
            switching.phase,
            escape_terminal(switching.target.as_str())
        )
    } else if !state.pending_requests.is_empty() {
        format!(
            "{} UI request(s) pending · changing rooms blocked · detach can cancel/interrupt work",
            state.pending_requests.len()
        )
    } else if state.form_busy {
        "Checking latest authority / sending · draft locked".to_owned()
    } else if let Some(mutation) = &state.mutation {
        match mutation.stage {
            MutationStage::Uncertain => match mutation.kind {
                FormKind::WorkspaceCreate => "Creation outcome unknown · Esc dismisses without resend; inspect workspace pages".to_owned(),
                FormKind::TaskRequest => "Request outcome unknown · inspect task/history; do not resend".to_owned(),
                _ => "Outcome uncertain · Ctrl-S retries the same immutable operation".to_owned(),
            },
            MutationStage::Conflict => format!(
                "Task conflict · current version {:?} · Ctrl-R reconfirms latest, Ctrl-S submits",
                mutation.current_version
            ),
            MutationStage::InFlight => "Mutation in flight…".to_owned(),
            _ => "Router stays running when you leave. Reattach with asr ui.".to_owned(),
        }
    } else {
        "Router stays running when you leave. Reattach with asr ui.".to_owned()
    };
    frame.render_widget(
        Paragraph::new(vec![
            Line::styled(preview(&status, usize::from(area.width)), theme.warning()),
            Line::styled(
                "F1 Chat · F2 Tasks · F3 Members · F4 Invite · Tab focus · Enter detail · ? help",
                theme.muted(),
            ),
        ]),
        area,
    );
}

fn render_modal(frame: &mut Frame<'_>, area: Rect, state: &UiState, modal: &Modal, theme: Theme) {
    let width = area.width.saturating_sub(6).min(100);
    let height = area.height.saturating_sub(4);
    let rect = Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    );
    frame.render_widget(Clear, rect);
    match modal {
        Modal::Form(form) => render_form(frame, rect, state, form, theme),
        Modal::Detail { kind, scroll } => {
            render_detail(frame, rect, state, Some(*kind), *scroll, theme);
        }
        Modal::Help { scroll } => {
            let block = pane(" Help · ↑/↓ scroll · Esc closes ", true, theme);
            let inner = block.inner(rect);
            frame.render_widget(block, rect);
            let mut window = TextWindow::new(inner.width, inner.height, *scroll);
            window.push(HELP);
            frame.render_widget(Paragraph::new(window.lines), inner);
        }
        Modal::Detach {
            confirmation,
            scroll,
        } => {
            let mut lines = vec![
                "Leave this UI connection? The server stays running.".to_owned(),
                "Disconnecting can cancel or interrupt requests made by this connection."
                    .to_owned(),
                "Requests from other connections are not deliberately cancelled.".to_owned(),
                "Default: return to the console (Esc).".to_owned(),
            ];
            for request in state.pending_requests.values() {
                lines.push(format!(
                    "Task #{} · request {} · {}",
                    request.task_id,
                    request.request_id,
                    request_stage(request.stage)
                ));
            }
            lines.push("Type DETACH and press Ctrl-S to confirm.".to_owned());
            render_confirmation(
                frame,
                rect,
                " Detach · ↑/↓ scroll pending requests ",
                &lines,
                confirmation,
                *scroll,
                theme,
            );
        }
        Modal::Stop { confirmation } => {
            let lines = vec![
                "Stop the owned router, not just this screen?".to_owned(),
                "Work in other workspaces can also be affected.".to_owned(),
                display_endpoint(&state.header.endpoint),
                format!(
                    "Owned instance: {}",
                    state
                        .header
                        .instance_id
                        .map_or_else(|| "not available".to_owned(), |id| id.to_string())
                ),
                if state.can_stop() {
                    "Ownership and instance must be rechecked before stopping.".to_owned()
                } else {
                    "Not an owned-admin connection. Stop it from the managing server.".to_owned()
                },
                "Default: return (Esc). Type STOP and press Ctrl-S to confirm.".to_owned(),
            ];
            render_confirmation(frame, rect, " Stop router ", &lines, confirmation, 0, theme);
        }
        Modal::InviteReady {
            workspace,
            provider,
            expires_at,
        } => {
            let block = pane(" Client invitation · token hidden ", true, theme);
            let inner = block.inner(rect);
            frame.render_widget(block, rect);
            let mut window = TextWindow::new(inner.width, inner.height, 0);
            window.field("Workspace", workspace.as_str());
            window.field(
                "Provider",
                provider
                    .as_deref()
                    .unwrap_or("one of Claude Code / Codex CLI / OMP"),
            );
            window.field("Expires", &utc_timestamp(*expires_at));
            window.push("The prompt contains a 10-minute, one-use invitation token.");
            window.push("Anyone who sees it before expiry may redeem it first.");
            window.push("p: explicitly print the exact prompt outside the alternate screen.");
            window.push("y: explicitly copy with an available OS clipboard helper.");
            window.push("No helper? Use p. Clipboard installation and OSC52 are not automatic.");
            window.push("Esc closes and discards this UI's prompt buffer.");
            window.push(
                "Installation is not activation: verify through the provider's real MCP tools.",
            );
            frame.render_widget(Paragraph::new(window.lines), inner);
        }
    }
}

fn render_form(frame: &mut Frame<'_>, area: Rect, state: &UiState, form: &FormState, theme: Theme) {
    let block = pane(
        format!(" {} · Esc cancels ", form_title(form.kind)),
        true,
        theme,
    );
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let sections = Layout::vertical([
        Constraint::Length(4),
        Constraint::Min(1),
        Constraint::Length(3),
    ])
    .split(inner);
    let mut context = vec![Line::styled(form_hint(form.kind), theme.muted())];
    if let Some(baseline) = &form.baseline {
        context.push(Line::from(format!(
            "Task #{} · {} v{} · assignee: {}",
            baseline.task_id,
            if form.kind == FormKind::TaskNote {
                "observed"
            } else {
                "expected"
            },
            baseline.version,
            optional_text(baseline.assigned_agent_id.as_deref())
        )));
        if form.kind == FormKind::TaskConfirmStopped {
            context.push(Line::from(format!(
                "Exact attempt: {}",
                baseline
                    .attempt_id
                    .map_or_else(|| "none".to_owned(), |attempt| attempt.to_string())
            )));
        }
    }
    if let Some(changed) = &form.changed {
        context.push(Line::styled(
            format!(
                "CHANGED: v{} · assignee {} · attempt {:?}",
                changed.version,
                optional_text(changed.assigned_agent_id.as_deref()),
                changed.attempt_id
            ),
            theme.warning(),
        ));
        context.push(Line::styled(
            "Draft preserved. Ctrl-R reconfirms latest; Ctrl-S rechecks and submits.",
            theme.warning(),
        ));
    }
    frame.render_widget(Paragraph::new(context), sections[0]);
    let checkbox_height = u16::from(form.kind == FormKind::TaskConfirmStopped);
    let body = Rect {
        height: sections[1].height.saturating_sub(checkbox_height),
        ..sections[1]
    };
    let desired = |index: usize| {
        if form.fields[index].input.multiline() {
            6_u16
        } else {
            3_u16
        }
    };
    let focused = form.focused.min(form.fields.len().saturating_sub(1));
    let mut first = 0;
    while first < focused && (first..=focused).map(desired).sum::<u16>() > body.height {
        first += 1;
    }
    let mut y = body.y;
    for (index, field) in form.fields.iter().enumerate().skip(first) {
        let remaining = body.bottom().saturating_sub(y);
        if remaining < 2 {
            break;
        }
        let height = desired(index).min(remaining);
        let field_area = Rect::new(body.x, y, body.width, height);
        let selected = form.focused == index;
        let label = format!(
            " {} · {}/{} B ",
            field.label,
            field.input.text().len(),
            field.input.max_bytes()
        );
        let block = pane(label, selected, theme);
        let input_area = block.inner(field_area);
        frame.render_widget(block, field_area);
        let locked = state.form_busy
            || state.mutation.as_ref().is_some_and(|mutation| {
                matches!(
                    mutation.stage,
                    MutationStage::InFlight | MutationStage::Uncertain
                )
            });
        render_input(frame, input_area, &field.input, selected && !locked, theme);
        y = y.saturating_add(height);
    }
    if form.kind == FormKind::TaskAssign && body.bottom().saturating_sub(y) >= 3 {
        let picker_area = Rect::new(body.x, y, body.width, body.bottom() - y);
        let block = pane(
            " ↑/↓ pick member or unassign · type an offline ID ",
            false,
            theme,
        );
        let picker_inner = block.inner(picker_area);
        frame.render_widget(block, picker_area);
        let selected = form.assignee_choice.map_or(0, |index| index + 1);
        let count = state.members.len() + 1;
        let visible = usize::from(picker_inner.height).max(1);
        let start = selected
            .saturating_sub(visible / 2)
            .min(count.saturating_sub(visible));
        let rows = (start..count)
            .take(visible)
            .map(|index| {
                ListItem::new(if index == 0 {
                    "Unassigned (no execution is started)".to_owned()
                } else {
                    preview(
                        &state.members[index - 1].agent_id,
                        usize::from(picker_inner.width.saturating_sub(2)),
                    )
                })
            })
            .collect::<Vec<_>>();
        let mut selection =
            ListState::default().with_selected(Some(selected.saturating_sub(start)));
        frame.render_stateful_widget(
            List::new(rows)
                .highlight_symbol("> ")
                .highlight_style(theme.selected()),
            picker_inner,
            &mut selection,
        );
    }
    if form.kind == FormKind::TaskConfirmStopped {
        let row = Rect::new(
            sections[1].x,
            sections[1].bottom().saturating_sub(1),
            sections[1].width,
            1,
        );
        let marker = if form.stopped_observed { "x" } else { " " };
        frame.render_widget(
            Paragraph::new(format!(
                "[{marker}] I observed actual execution stop · Tab focuses, Space toggles"
            ))
            .style(if form.focused == form.fields.len() {
                theme.selected()
            } else {
                theme.warning()
            }),
            row,
        );
    }
    let error = form
        .error
        .map(state_error)
        .or_else(|| form.router_error.map(RouterErrorCode::as_str))
        .unwrap_or("");
    let action = if state.form_busy {
        "Checking latest authority / sending: draft locked; please wait."
    } else if state.mutation.as_ref().is_some_and(|mutation| {
        mutation.stage == MutationStage::Uncertain && mutation.kind == FormKind::WorkspaceCreate
    }) {
        "Outcome unknown: Esc dismisses and refreshes the list; no resend."
    } else if state.mutation.as_ref().is_some_and(|mutation| {
        mutation.stage == MutationStage::Uncertain && mutation.kind == FormKind::TaskRequest
    }) {
        "Outcome uncertain: no resend. Esc, then inspect authoritative state."
    } else if state
        .mutation
        .as_ref()
        .is_some_and(|mutation| mutation.stage == MutationStage::Uncertain)
    {
        "Outcome uncertain: Ctrl-S retries the same immutable operation."
    } else if form.changed.is_some() {
        "Ctrl-R reconfirms latest · Ctrl-S rechecks and sends a NEW operation"
    } else {
        "Ctrl-S submits · Tab/Shift-Tab fields · multiline Enter inserts newline"
    };
    frame.render_widget(
        Paragraph::new(vec![
            Line::styled(error, theme.warning()),
            Line::from(action),
            Line::styled(
                "Single-line Enter advances · paste only edits the buffer",
                theme.muted(),
            ),
        ]),
        sections[2],
    );
}

fn render_confirmation(
    frame: &mut Frame<'_>,
    area: Rect,
    title: &'static str,
    lines: &[String],
    input: &InputBuffer,
    scroll: usize,
    theme: Theme,
) {
    let block = pane(title, true, theme);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let regions = Layout::vertical([Constraint::Min(1), Constraint::Length(3)]).split(inner);
    let mut window = TextWindow::new(regions[0].width, regions[0].height, scroll);
    for line in lines {
        window.push(line);
    }
    frame.render_widget(Paragraph::new(window.lines), regions[0]);
    let block = pane(" Confirmation · Ctrl-S submits · Esc returns ", true, theme);
    let input_area = block.inner(regions[1]);
    frame.render_widget(block, regions[1]);
    render_input(frame, input_area, input, true, theme);
}

fn render_input(
    frame: &mut Frame<'_>,
    area: Rect,
    input: &InputBuffer,
    focused: bool,
    theme: Theme,
) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let (cursor_row, cursor_column) = input_cursor(input, usize::from(area.width));
    let first_row = cursor_row.saturating_sub(usize::from(area.height.saturating_sub(1)));
    let mut window = TextWindow::new(area.width, area.height, first_row);
    window.push(input.text());
    frame.render_widget(
        Paragraph::new(window.lines).style(if focused {
            Style::default()
        } else {
            theme.muted()
        }),
        area,
    );
    if focused {
        let x = area.x.saturating_add(
            u16::try_from(cursor_column)
                .unwrap_or(area.width - 1)
                .min(area.width - 1),
        );
        let y = area.y.saturating_add(
            u16::try_from(cursor_row - first_row)
                .unwrap_or(area.height - 1)
                .min(area.height - 1),
        );
        frame.set_cursor_position((x, y));
    }
}

fn input_cursor(input: &InputBuffer, width: usize) -> (usize, usize) {
    let mut row = 0;
    let mut column = 0;
    for grapheme in input.text()[..input.cursor()].graphemes(true) {
        if grapheme == "\n" {
            row += 1;
            column = 0;
            continue;
        }
        let escaped = safe_grapheme(grapheme);
        for displayed in escaped.graphemes(true) {
            let cells = UnicodeWidthStr::width(displayed);
            if column + cells > width {
                row += 1;
                column = 0;
            }
            column += cells;
        }
    }
    if column >= width {
        (row + 1, 0)
    } else {
        (row, column)
    }
}

/// Escape all controls using the CLI convention, retaining LF only as a line
/// delimiter. This helper is also useful to the runtime's explicit prompt-free
/// diagnostics; token-bearing prompts must never pass through ordinary logging.
#[must_use]
pub fn escape_multiline(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for (index, line) in value.split('\n').enumerate() {
        if index > 0 {
            escaped.push('\n');
        }
        escaped.push_str(&escape_terminal(line));
    }
    escaped
}

fn safe_grapheme(value: &str) -> Cow<'_, str> {
    if value
        .chars()
        .any(|character| character <= '\u{001f}' || ('\u{007f}'..='\u{009f}').contains(&character))
    {
        Cow::Owned(escape_terminal(value))
    } else {
        Cow::Borrowed(value)
    }
}

/// Bounded display-width preview. Input is escaped before it reaches a widget;
/// a long or malicious message never allocates its full escaped body for a row.
#[must_use]
pub fn preview(value: &str, cells: usize) -> String {
    if cells == 0 {
        return String::new();
    }
    let mut output = String::new();
    let mut used = 0;
    let mut graphemes = value.graphemes(true).peekable();
    while let Some(grapheme) = graphemes.next() {
        if output.len().saturating_add(grapheme.len()) > 1_024 {
            output.push('…');
            break;
        }
        let escaped = if grapheme == "\n" {
            Cow::Borrowed(" ↵ ")
        } else {
            safe_grapheme(grapheme)
        };
        let width = UnicodeWidthStr::width(escaped.as_ref());
        let reserve = usize::from(graphemes.peek().is_some());
        if used + width + reserve > cells {
            output.push('…');
            break;
        }
        output.push_str(&escaped);
        used += width;
    }
    output
}

/// A line window wraps escaped text itself, retaining only visible rows. Unlike
/// `Paragraph::scroll` over a giant owned Text this cannot retain a second copy of
/// a full chat page, and it stops scanning once the viewport is filled.
struct TextWindow {
    width: usize,
    height: usize,
    skip: usize,
    row: usize,
    lines: Vec<Line<'static>>,
}

impl TextWindow {
    fn new(width: u16, height: u16, skip: usize) -> Self {
        Self {
            width: usize::from(width).max(1),
            height: usize::from(height),
            skip,
            row: 0,
            lines: Vec::with_capacity(usize::from(height)),
        }
    }
    fn full(&self) -> bool {
        self.lines.len() >= self.height
    }
    fn line(&mut self, text: String) {
        if self.row >= self.skip && !self.full() {
            self.lines.push(Line::from(text));
        }
        self.row = self.row.saturating_add(1);
    }
    fn field(&mut self, label: &str, value: &str) {
        if self.full() {
            return;
        }
        self.push(label);
        self.push(value);
    }
    fn push(&mut self, value: &str) {
        if self.full() {
            return;
        }
        // Split LF before segmenting graphemes, so a CRLF cluster still renders
        // the CR escaped and the LF as a line break.
        for logical_line in value.split('\n') {
            let mut text = String::new();
            let mut width = 0;
            for grapheme in logical_line.graphemes(true) {
                let escaped = safe_grapheme(grapheme);
                for displayed in escaped.graphemes(true) {
                    let cells = UnicodeWidthStr::width(displayed);
                    if width > 0 && width + cells > self.width {
                        self.line(std::mem::take(&mut text));
                        width = 0;
                        if self.full() {
                            return;
                        }
                    }
                    if self.row >= self.skip {
                        text.push_str(displayed);
                    }
                    width += cells;
                }
            }
            self.line(text);
            if self.full() {
                return;
            }
        }
    }
}

fn optional_text(value: Option<&str>) -> String {
    value.map_or_else(|| "—".to_owned(), escape_terminal)
}
fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn event_kind(kind: WorkspaceEventKind) -> &'static str {
    match kind {
        WorkspaceEventKind::Chat => "CHAT",
        WorkspaceEventKind::Request => "REQUEST",
        WorkspaceEventKind::Result => "RESULT",
        WorkspaceEventKind::MemberJoined => "JOINED",
        WorkspaceEventKind::MemberLeft => "LEFT",
        WorkspaceEventKind::Task => "TASK",
        WorkspaceEventKind::Integration => "INTEGRATION",
    }
}

fn event_preview(event: &WorkspaceEvent, width: usize) -> String {
    let content = event.content.as_deref().unwrap_or("");
    if event.kind == WorkspaceEventKind::Task
        && let Ok(task_event) = serde_json::from_str::<crate::tasks::TaskEvent>(content)
    {
        return preview(
            &format!(
                "{:?} #{} {} · assigned {} · executor {} · v{}",
                task_event.change,
                task_event.task.id,
                task_event.task.state.as_str(),
                task_event
                    .task
                    .assigned_agent_id
                    .as_deref()
                    .unwrap_or("none"),
                task_event
                    .task
                    .last_executor_id
                    .as_deref()
                    .unwrap_or("none"),
                task_event.task.version
            ),
            width,
        );
    }
    preview(content, width)
}

fn event_route(event: &WorkspaceEvent) -> String {
    let mut parts = Vec::with_capacity(4);
    if let Some(target) = &event.target_id {
        parts.push(format!("→ {}", escape_terminal(target)));
    }
    if let Some(request) = &event.request_id {
        parts.push(format!("request {}", escape_terminal(request)));
    }
    if let Some(task) = event.task_id {
        parts.push(format!("task #{task}"));
    }
    if let Some(ok) = event.ok {
        parts.push(if ok {
            "OK".to_owned()
        } else {
            "ERROR".to_owned()
        });
    }
    if let Some(error) = event.error {
        parts.push(error.as_str().to_owned());
    }
    if parts.is_empty() {
        parts.push("Enter: full content".to_owned());
    }
    parts.join(" · ")
}

fn request_stage(stage: RequestStage) -> &'static str {
    match stage {
        RequestStage::Sending => "request sending; execution not yet confirmed",
        RequestStage::Accepted => "accepted; waiting for execution evidence",
        RequestStage::ExecutionObserved => "execution observed; request result still pending",
        RequestStage::Uncertain => {
            "transport outcome uncertain; inspect task/history, do not resend"
        }
    }
}

#[must_use]
pub fn state_error(error: StateError) -> &'static str {
    match error {
        StateError::NoWorkspace => "Select a workspace first.",
        StateError::Busy => "Finish or cancel the current operation before continuing.",
        StateError::NotConnected => "Not connected; mutations are disabled.",
        StateError::Synchronizing => "Synchronizing history; wait before writing.",
        StateError::TerminalTooSmall => "Resize the terminal to at least 80×24 before writing.",
        StateError::PermissionDenied => "This action requires server-administrator permission.",
        StateError::InvalidPage => "Invalid history page; synchronization stopped.",
        StateError::InvalidInput => "Invalid input; correct the form before submitting.",
        StateError::InputTooLarge => "Input exceeds the field's byte limit; nothing was inserted.",
        StateError::SingleLine => "This field accepts one line only; paste was not submitted.",
        StateError::MissingTask => "Refresh and select an authoritative task detail.",
        StateError::ReconfirmationRequired => {
            "Refresh task detail, then Ctrl-R reconfirms and Ctrl-S submits."
        }
        StateError::MustObserveStopped => {
            "Confirm that actual execution stop was observed, and provide a note."
        }
        StateError::StopUnconfirmed => {
            "Stop evidence does not permit this action; refresh task detail."
        }
        StateError::RequestNotRetryable => {
            "Do not resend this request; inspect authoritative state first."
        }
        StateError::NotUncertain => "Only an uncertain operation can be explicitly retried.",
        StateError::WrongReceipt => {
            "Receipt does not match the immutable operation; no state applied."
        }
    }
}

fn form_title(kind: FormKind) -> &'static str {
    match kind {
        FormKind::WorkspaceCreate => "Create workspace",
        FormKind::TaskCreate => "Create task",
        FormKind::TaskEdit => "Edit task",
        FormKind::TaskAssign => "Assign task (does not execute)",
        FormKind::TaskRequest => "Request task execution",
        FormKind::TaskInterrupt => "Interrupt task",
        FormKind::TaskConfirmStopped => "Confirm observed execution stop",
        FormKind::TaskNote => "Add task note",
        FormKind::TaskCancel => "Cancel task",
        FormKind::TaskReopen => "Reopen task",
        FormKind::TaskFilter => "Task filters",
        FormKind::Invite => "Invite client to selected workspace",
    }
}

fn form_hint(kind: FormKind) -> &'static str {
    match kind {
        FormKind::WorkspaceCreate => {
            "Admin only. Creation does not bypass workspace-switch fences."
        }
        FormKind::TaskCreate => "Creates a task only. Assign and request execution separately.",
        FormKind::TaskAssign => {
            "Choose a member ID or type an offline ID; the server validates its grant."
        }
        FormKind::TaskRequest => "Accepted is not begun. Requests are not automatically resent.",
        FormKind::TaskInterrupt => {
            "Interrupt requires handoff. Unknown stop evidence blocks new execution."
        }
        FormKind::TaskConfirmStopped => {
            "Do not infer stop from disconnect or elapsed time. Actual observation required."
        }
        FormKind::TaskCancel => {
            "Confirm cancellation with a note; existing execution fences remain enforced."
        }
        FormKind::TaskReopen => "Reopening does not assign or start an execution.",
        FormKind::TaskNote => "Adds a report note. No artificial expected-version fence.",
        FormKind::TaskFilter => "All six states by default. Empty assignee means all identities.",
        FormKind::Invite => {
            "Only submit issues an invitation. No invitation token is rendered in this form."
        }
        FormKind::TaskEdit => {
            "Version checked immediately before submit; conflicts preserve this draft."
        }
    }
}

fn utc_clock(millis: i64) -> String {
    let seconds = millis.div_euclid(1_000).rem_euclid(86_400);
    format!(
        "{:02}:{:02}:{:02} UTC",
        seconds / 3_600,
        (seconds / 60) % 60,
        seconds % 60
    )
}

/// Gregorian civil-date conversion avoids local-time/uptime assumptions and an
/// additional time dependency. i128 intermediates cover every wire i64 timestamp.
#[must_use]
pub fn utc_timestamp(millis: i64) -> String {
    let days = i128::from(millis.div_euclid(86_400_000));
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i128::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02} {}", utc_clock(millis))
}

const HELP: &str = "ASR console\n\
The server and this screen have separate lifetimes. q/Ctrl-C detaches; asr ui reconnects.\n\
If this connection has pending task requests, leaving needs literal DETACH confirmation. Disconnect may cancel or interrupt those requests. Other connections' requests are not deliberately cancelled.\n\
Ctrl-X opens owned-server stop confirmation. Type STOP and Ctrl-S. This can affect every workspace. Remote viewers must stop from the managing server.\n\
Navigation\n\
F1 Chat · F2 Tasks · F3 Members · Tab / Shift-Tab focus · arrows / PageUp / PageDown navigate · Enter full detail · Esc close modal · ? help.\n\
Workspace sidebar\n\
Enter selects after unsubscribe → leave → join succeeds. [ / ] pages in groups of 100. Ctrl-N creates (admin only). F4 invites (owned admin only). A pending request, operation or draft must be settled before switching.\n\
Chat\n\
i or Tab focuses composer. Ctrl-S posts ordinary chat; chat does NOT automatically execute a model. Enter adds a newline. Input clears only after the server's posted acknowledgement or matching durable event.\n\
e switches primary/all events. Up at the first row, PageUp or [ loads previous history; ] or PageDown loads the next past page. End reloads recent history and follows live events. Past pages and live ACK progress are separate. Enter shows full original text, safely escaped.\n\
Tasks\n\
Ctrl-T create · a assign · r request · i interrupt · f confirm stopped · e edit · m note · c cancel · o reopen · / state/assignee filters · [ / ] pages · h task history.\n\
Assign: Up/Down picks a connected member or unassigned; type an ID for offline identities. Assign does not run work. Accepted does not mean begun. A request result does not mean task completion. Authoritative task versions and attempt state determine what happened.\n\
The assignee and actual executor are distinct. Interrupted + unknown stop evidence blocks a new execution. Confirm-stopped requires the exact attempt, an observed-stop checkbox and a note. Operators cannot begin/checkpoint/pause/complete as an executor.\n\
All six states are included by default. Task/history pagination uses [ / ] forward cursors; no total page count or fake latest-history claim.\n\
Forms\n\
Tab/Shift-Tab changes fields. Enter advances single-line fields or adds a newline in multiline text. Ctrl-S is the only submit key. Paste only inserts text. Grapheme-aware arrows/delete preserve Korean, emoji and combining characters.\n\
Task edits/assignment/request/interrupt/cancel/reopen/confirm-stopped require a fresh task_get before submit. The draft is locked during that check and while in flight. A changed version keeps your draft: Ctrl-R explicitly reconfirms the latest baseline; Ctrl-S rechecks and submits a NEW operation. The old operation is never silently retargeted.\n\
Ctrl-S on an uncertain mutation or chat retries its same ID and immutable payload. TaskRequest and WorkspaceCreate cannot be retried. Esc dismisses unknown workspace creation without resending and refreshes its list; inspect all relevant pages before creating again. Receipt-backed mutation payloads remain available for exact retry after closing their form. Inspect task state/history after a lost request response; there is no automatic resend.\n\
Members\n\
Only connected agents appear. Offline identities are absent; operator viewers do not increase agent counts. Ready is the server-reported readiness flag.\n\
Invitations\n\
F4 opens the invite form; Ctrl-S issues once and hides the short-lived token. Only the ready modal enables p (print outside the alternate screen) and y (explicit OS clipboard copy); no auto-install or OSC52. Esc discards the UI prompt buffer.\n\
Installation and activation differ. Verify participation using the new provider session's actual workspace_list and workspace_members tools.\n\
Safety\n\
Reconnecting/synchronizing disables mutations. No live route hot-swap is performed here. Below 80×24 the connection is retained but mutations are disabled. NO_COLOR removes colors without removing labels. Untrusted C0/DEL/C1/ESC/OSC are escaped; only LF is a line separator.\n\
Task reports and chat are user-authored text. The console does not claim to detect secrets users paste into their own messages.";
