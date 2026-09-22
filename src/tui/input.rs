//! Keyboard and paste transitions. Network commands never run in the input loop.
use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use super::{
    UiCommand, UiInput,
    state::{
        Connection, DetailKind, FieldId, Focus, FormKind, FormState, HistoryMode, InputBuffer,
        Modal, MutationStage, Notice, StateError, Tab, UiState, WIDE_WIDTH,
    },
    view::state_error,
};

pub fn handle(state: &mut UiState, event: Event) -> UiInput {
    if state.detaching {
        return UiInput::None;
    }
    match event {
        Event::Resize(width, height) => {
            state.resize(width, height);
            UiInput::None
        }
        Event::Paste(text) => paste(state, &text),
        Event::Key(key) if key.kind != KeyEventKind::Release => {
            let key = normalize_alias(key);
            state.mark_dirty();
            if control(key, 'c') {
                return request_detach(state);
            }
            if state.modal.is_some() {
                return modal_key(state, key);
            }
            if state.focus == Focus::Composer {
                return composer_key(state, key);
            }
            navigation_key(state, key)
        }
        _ => UiInput::None,
    }
}

/// SIGINT and Ctrl-C use the same pending-request ownership fence.
pub fn request_detach(state: &mut UiState) -> UiInput {
    if !state.detach_needs_confirmation() {
        return accept_detach(state);
    }
    if !matches!(state.modal, Some(Modal::Detach { .. })) {
        state.suspended_modal = state.modal.take();
        state.modal = Some(Modal::Detach {
            confirmation: InputBuffer::new(6, false),
            scroll: 0,
        });
    }
    state.mark_dirty();
    UiInput::None
}

fn accept_detach(state: &mut UiState) -> UiInput {
    state.detaching = true;
    state.mark_dirty();
    UiInput::Detach
}

/// A full command queue must not leave a draft frozen without an owner.
pub fn command_not_sent(state: &mut UiState, command: &UiCommand) {
    if matches!(
        command,
        UiCommand::SubmitForm
            | UiCommand::SubmitChat
            | UiCommand::RetryMutation
            | UiCommand::RetryChat
            | UiCommand::ConfirmStop
    ) {
        state.form_busy = false;
    }
    fail(state, StateError::Busy);
}

fn normalize_alias(mut key: KeyEvent) -> KeyEvent {
    if control(key, 'i') {
        key.code = KeyCode::Tab;
        key.modifiers.remove(KeyModifiers::CONTROL);
    } else if control(key, 'j') {
        key.code = KeyCode::Enter;
        key.modifiers.remove(KeyModifiers::CONTROL);
    }
    key
}

fn control(key: KeyEvent, letter: char) -> bool {
    key.modifiers.contains(KeyModifiers::CONTROL)
        && !key
            .modifiers
            .intersects(KeyModifiers::ALT | KeyModifiers::SUPER)
        && matches!(key.code, KeyCode::Char(value) if value.eq_ignore_ascii_case(&letter))
}

fn plain(key: KeyEvent) -> bool {
    !key.modifiers
        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER)
}

fn fail(state: &mut UiState, error: StateError) -> UiInput {
    if let Some(form) = state.form_mut() {
        form.error = Some(error);
    }
    state.notice = Some(Notice {
        message: state_error(error),
        error: None,
    });
    state.mark_dirty();
    UiInput::None
}

fn transport_ready(state: &UiState, workspace_required: bool) -> Result<(), StateError> {
    if !state.terminal_large_enough() {
        return Err(StateError::TerminalTooSmall);
    }
    if !matches!(state.connection, Connection::Connected { .. }) {
        return Err(StateError::NotConnected);
    }
    if state.detaching || state.switching.is_some() || state.form_busy {
        return Err(StateError::Busy);
    }
    if workspace_required && state.workspace.is_none() {
        return Err(StateError::NoWorkspace);
    }
    if state.workspace.is_some() && !state.chat.synchronized {
        return Err(StateError::Synchronizing);
    }
    Ok(())
}

fn draft_locked(state: &UiState) -> bool {
    state.form_busy
        || state.mutation.as_ref().is_some_and(|mutation| {
            matches!(
                mutation.stage,
                MutationStage::InFlight | MutationStage::Uncertain
            )
        })
}

fn submit_form(state: &mut UiState) -> UiInput {
    if state
        .mutation
        .as_ref()
        .is_some_and(|mutation| mutation.stage == MutationStage::Uncertain)
    {
        return retry_mutation(state);
    }
    let Some(form) = state.form() else {
        return UiInput::None;
    };
    let kind = form.kind;
    if let Err(error) = transport_ready(state, kind != FormKind::WorkspaceCreate) {
        return fail(state, error);
    }
    if draft_locked(state) || state.composer.is_locked() {
        return fail(state, StateError::Busy);
    }
    if kind == FormKind::WorkspaceCreate && !state.header.is_admin
        || kind == FormKind::Invite && !state.can_invite()
    {
        return fail(state, StateError::PermissionDenied);
    }
    if form.changed.is_some()
        || state
            .mutation
            .as_ref()
            .is_some_and(|mutation| mutation.stage == MutationStage::Conflict)
    {
        return fail(state, StateError::ReconfirmationRequired);
    }
    if kind == FormKind::TaskFilter {
        match form.task_filter() {
            Ok(filter) => {
                state.tasks.set_filter(filter);
                state.close_form();
                state.notice = None;
                return UiInput::Command(UiCommand::LoadTasks);
            }
            Err(error) => return fail(state, error),
        }
    }
    state.notice = None;
    state.form_busy = true;
    UiInput::Command(UiCommand::SubmitForm)
}

fn retry_mutation(state: &mut UiState) -> UiInput {
    let Some(mutation) = state.mutation.as_ref() else {
        return UiInput::None;
    };
    if mutation.stage != MutationStage::Uncertain {
        return fail(state, StateError::Busy);
    }
    if matches!(
        mutation.kind,
        FormKind::WorkspaceCreate | FormKind::TaskRequest
    ) {
        return fail(state, StateError::RequestNotRetryable);
    }
    if !state.accepts(&mutation.stamp) {
        return fail(state, StateError::NoWorkspace);
    }
    if let Err(error) = transport_ready(state, true) {
        return fail(state, error);
    }
    if state.composer.is_locked() {
        return fail(state, StateError::Busy);
    }
    state.form_busy = true;
    state.notice = None;
    UiInput::Command(UiCommand::RetryMutation)
}

fn submit_chat(state: &mut UiState) -> UiInput {
    if let Err(error) = transport_ready(state, true) {
        return fail(state, error);
    }
    if draft_locked(state) {
        return fail(state, StateError::Busy);
    }
    let command = if let Some(post) = state.composer.pending() {
        if !post.uncertain {
            return fail(state, StateError::Busy);
        }
        UiCommand::RetryChat
    } else {
        if let Err(error) = state.writable() {
            return fail(state, error);
        }
        if state.composer.input.text().is_empty() {
            return fail(state, StateError::InvalidInput);
        }
        UiCommand::SubmitChat
    };
    state.form_busy = true;
    state.notice = None;
    UiInput::Command(command)
}

fn open_form(state: &mut UiState, kind: FormKind) -> UiInput {
    if kind == FormKind::WorkspaceCreate && !state.header.is_admin
        || kind == FormKind::Invite && !state.can_invite()
    {
        return fail(state, StateError::PermissionDenied);
    }
    if let Err(error) = transport_ready(state, kind != FormKind::WorkspaceCreate) {
        return fail(state, error);
    }
    if draft_locked(state) || state.composer.is_locked() {
        return fail(state, StateError::Busy);
    }
    let needs_task = !matches!(
        kind,
        FormKind::WorkspaceCreate | FormKind::TaskCreate | FormKind::TaskFilter | FormKind::Invite
    );
    if needs_task
        && (state.tasks.detail_stale
            || state.tasks.detail.as_ref().is_none_or(|detail| {
                Some(detail.summary.id) != state.tasks.selected_id
                    || state
                        .workspace
                        .as_ref()
                        .is_none_or(|workspace| detail.summary.workspace != workspace.as_str())
            }))
    {
        fail(state, StateError::MissingTask);
        return if state.tasks.selected_id.is_some() {
            UiInput::Command(UiCommand::LoadTaskDetail)
        } else {
            UiInput::None
        };
    }
    if kind == FormKind::TaskRequest && !state.tasks.can_request()
        || kind == FormKind::TaskConfirmStopped && state.tasks.confirmable_attempt().is_none()
    {
        return fail(state, StateError::StopUnconfirmed);
    }
    let mut form = match FormState::new(kind, state.tasks.detail.as_ref()) {
        Ok(form) => form,
        Err(error) => return fail(state, error),
    };
    if kind == FormKind::TaskFilter {
        let states = state
            .tasks
            .filter
            .states
            .iter()
            .map(|state| state.as_str())
            .collect::<Vec<_>>()
            .join(",");
        for field in &mut form.fields {
            let value = match field.id {
                FieldId::States => states.as_str(),
                FieldId::Agent => state
                    .tasks
                    .filter
                    .assigned_agent_id
                    .as_deref()
                    .unwrap_or(""),
                _ => "",
            };
            if let Err(error) = field.input.insert(value) {
                return fail(state, error);
            }
        }
    }
    state.mutation = None;
    state.notice = None;
    state.modal = Some(Modal::Form(form));
    if kind == FormKind::TaskAssign {
        UiInput::Command(UiCommand::LoadMembers)
    } else {
        UiInput::None
    }
}

fn modal_key(state: &mut UiState, key: KeyEvent) -> UiInput {
    if matches!(state.modal, Some(Modal::Form(_))) {
        return form_key(state, key);
    }
    if key.code == KeyCode::Char('q')
        && plain(key)
        && matches!(
            state.modal,
            Some(Modal::Help { .. } | Modal::Detail { .. } | Modal::InviteReady { .. })
        )
    {
        return request_detach(state);
    }
    if key.code == KeyCode::Esc {
        if matches!(state.modal, Some(Modal::InviteReady { .. })) {
            state.modal = None;
            return UiInput::DiscardPrompt;
        }
        if matches!(state.modal, Some(Modal::Detach { .. })) {
            state.modal = state.suspended_modal.take();
        } else if !state.form_busy {
            state.modal = None;
        }
        return UiInput::None;
    }
    if matches!(state.modal, Some(Modal::InviteReady { .. })) {
        return match key.code {
            KeyCode::Char('p') if plain(key) => UiInput::PrintPrompt,
            KeyCode::Char('y') if plain(key) => UiInput::CopyPrompt,
            _ => UiInput::None,
        };
    }
    if control(key, 's') {
        if matches!(state.modal, Some(Modal::Detach { .. })) {
            return if state.detach_confirmed() {
                accept_detach(state)
            } else {
                fail(state, StateError::InvalidInput)
            };
        }
        if matches!(state.modal, Some(Modal::Stop { .. })) {
            if !state.can_stop() {
                return fail(state, StateError::PermissionDenied);
            }
            if let Err(error) = transport_ready(state, false) {
                return fail(state, error);
            }
            if draft_locked(state) || state.composer.is_locked() {
                return fail(state, StateError::Busy);
            }
            if !state.stop_confirmed() {
                return fail(state, StateError::InvalidInput);
            }
            state.form_busy = true;
            return UiInput::Command(UiCommand::ConfirmStop);
        }
    }
    if matches!(
        state.modal,
        Some(Modal::Detail {
            kind: DetailKind::TaskHistory,
            ..
        })
    ) && plain(key)
    {
        let changed = match key.code {
            KeyCode::Char('[') => state.tasks.history_pagination.previous_page(),
            KeyCode::Char(']') => state.tasks.history_pagination.next_page(),
            _ => false,
        };
        if changed {
            if let Some(Modal::Detail { scroll, .. }) = &mut state.modal {
                *scroll = 0;
            }
            return UiInput::Command(UiCommand::LoadTaskHistory);
        }
    }
    let page = usize::from(state.height.saturating_sub(12)).max(1);
    let busy = state.form_busy;
    let result = match &mut state.modal {
        Some(Modal::Help { scroll } | Modal::Detail { scroll, .. }) => {
            scroll_key(scroll, key, page);
            Ok(())
        }
        Some(Modal::Detach {
            confirmation,
            scroll,
        }) => {
            if matches!(key.code, KeyCode::PageUp | KeyCode::PageDown) {
                scroll_key(scroll, key, page);
                Ok(())
            } else {
                edit_input(confirmation, key)
            }
        }
        Some(Modal::Stop { confirmation }) if !busy => edit_input(confirmation, key),
        _ => Ok(()),
    };
    if let Err(error) = result {
        return fail(state, error);
    }
    UiInput::None
}

fn form_key(state: &mut UiState, key: KeyEvent) -> UiInput {
    if control(key, 's') {
        return submit_form(state);
    }
    if key.code == KeyCode::Esc {
        if state.form_busy
            || state
                .mutation
                .as_ref()
                .is_some_and(|mutation| mutation.stage == MutationStage::InFlight)
        {
            return fail(state, StateError::Busy);
        }
        state.close_form();
        if state.mutation.as_ref().is_some_and(|mutation| {
            mutation.stage == MutationStage::Uncertain && mutation.kind == FormKind::WorkspaceCreate
        }) {
            state.mutation = None;
            state.notice = Some(Notice {
                message: "Workspace creation outcome remains unknown. Refresh and inspect workspace pages before creating again; nothing was resent.",
                error: None,
            });
            return UiInput::Command(UiCommand::LoadWorkspaces);
        }
        if state
            .mutation
            .as_ref()
            .is_some_and(|mutation| mutation.stage != MutationStage::Uncertain)
        {
            state.mutation = None;
        }
        return UiInput::None;
    }
    if draft_locked(state) {
        return fail(state, StateError::Busy);
    }
    if control(key, 'r') {
        let Some(form) = state.form_mut() else {
            return UiInput::None;
        };
        if let Err(error) = form.reconfirm_latest() {
            fail(state, error);
            return if state.tasks.selected_id.is_some() {
                UiInput::Command(UiCommand::LoadTaskDetail)
            } else {
                UiInput::None
            };
        }
        state.mutation = None;
        state.notice = None;
        return UiInput::None;
    }
    // Borrow disjoint state fields: member choice never invents an offline identity.
    let Some(Modal::Form(form)) = &mut state.modal else {
        return UiInput::None;
    };
    let result = match key.code {
        KeyCode::Tab => {
            form.next_field(key.modifiers.contains(KeyModifiers::SHIFT));
            Ok(())
        }
        KeyCode::BackTab => {
            form.next_field(true);
            Ok(())
        }
        KeyCode::Enter => form.enter(),
        KeyCode::Char(' ') if plain(key) && form.focused == form.fields.len() => {
            form.toggle_checkbox();
            Ok(())
        }
        KeyCode::Up | KeyCode::Down if form.kind == FormKind::TaskAssign && plain(key) => {
            let count = state.members.len() + 1;
            let current = form
                .assignee_choice
                .map_or(0, |index| index + 1)
                .min(count - 1);
            let next = if key.code == KeyCode::Down {
                (current + 1) % count
            } else {
                (current + count - 1) % count
            };
            form.choose_assignee(&state.members, next.checked_sub(1))
        }
        _ => {
            if form.kind == FormKind::TaskAssign
                && matches!(
                    key.code,
                    KeyCode::Char(_) | KeyCode::Backspace | KeyCode::Delete
                )
                && plain(key)
            {
                form.assignee_choice = None;
            }
            form.focused_input()
                .map_or(Ok(()), |input| edit_input(input, key))
        }
    };
    if let Err(error) = result {
        return fail(state, error);
    }
    UiInput::None
}

fn composer_key(state: &mut UiState, key: KeyEvent) -> UiInput {
    if control(key, 's') {
        return submit_chat(state);
    }
    if control(key, 'n') || control(key, 't') || control(key, 'x') {
        return navigation_key(state, key);
    }
    match key.code {
        KeyCode::Esc => {
            state.focus = Focus::Main;
            return UiInput::None;
        }
        KeyCode::Tab | KeyCode::BackTab => {
            cycle_focus(
                state,
                key.code == KeyCode::BackTab || key.modifiers.contains(KeyModifiers::SHIFT),
            );
            return UiInput::None;
        }
        KeyCode::F(1..=4) => return navigation_key(state, key),
        _ => {}
    }
    if state.composer.is_locked() || draft_locked(state) {
        return fail(state, StateError::Busy);
    }
    if let Err(error) = edit_input(&mut state.composer.input, key) {
        return fail(state, error);
    }
    UiInput::None
}

fn paste(state: &mut UiState, text: &str) -> UiInput {
    state.mark_dirty();
    if (matches!(state.modal, Some(Modal::Form(_)))
        || state.modal.is_none() && state.focus == Focus::Composer)
        && draft_locked(state)
    {
        return fail(state, StateError::Busy);
    }
    let result = match &mut state.modal {
        Some(Modal::Form(form)) => {
            if form.kind == FormKind::TaskAssign {
                form.assignee_choice = None;
            }
            form.focused_input()
                .map_or(Ok(()), |input| input.insert(text))
        }
        Some(Modal::Detach { confirmation, .. }) => confirmation.insert(text),
        Some(Modal::Stop { confirmation }) if !state.form_busy => confirmation.insert(text),
        None if state.focus == Focus::Composer => {
            if state.form_busy || state.composer.is_locked() {
                Err(StateError::Busy)
            } else {
                state.composer.input.insert(text)
            }
        }
        _ => Ok(()),
    };
    if let Err(error) = result {
        return fail(state, error);
    }
    UiInput::None
}

fn edit_input(input: &mut InputBuffer, key: KeyEvent) -> Result<(), StateError> {
    if !plain(key) {
        return Ok(());
    }
    match key.code {
        KeyCode::Char(value) => {
            input.insert(value.encode_utf8(&mut [0; 4]))?;
        }
        KeyCode::Enter if input.multiline() => {
            input.insert("\n")?;
        }
        KeyCode::Left => input.left(),
        KeyCode::Right => input.right(),
        KeyCode::Up => input.vertical(false),
        KeyCode::Down => input.vertical(true),
        KeyCode::Home => input.home(),
        KeyCode::End => input.end(),
        KeyCode::Backspace => input.backspace(),
        KeyCode::Delete => input.delete(),
        _ => {}
    }
    Ok(())
}

fn navigation_key(state: &mut UiState, key: KeyEvent) -> UiInput {
    if control(key, 's') {
        return retry_mutation(state);
    }
    if control(key, 'n') {
        return open_form(state, FormKind::WorkspaceCreate);
    }
    if control(key, 't') {
        return open_form(state, FormKind::TaskCreate);
    }
    if control(key, 'x') {
        if !state.can_stop() {
            state.notice = Some(Notice {
                message: "Stop from the managing server with asr router stop; this viewer cannot stop it.",
                error: None,
            });
            return UiInput::None;
        }
        if let Err(error) = transport_ready(state, false) {
            return fail(state, error);
        }
        if draft_locked(state) || state.composer.is_locked() {
            return fail(state, StateError::Busy);
        }
        state.modal = Some(Modal::Stop {
            confirmation: InputBuffer::new(4, false),
        });
        return UiInput::None;
    }
    if !plain(key) {
        return UiInput::None;
    }
    match key.code {
        KeyCode::F(1) => {
            state.tab = Tab::Chat;
            state.focus = Focus::Main;
            return UiInput::None;
        }
        KeyCode::F(2) => {
            state.tab = Tab::Tasks;
            state.focus = Focus::Main;
            return UiInput::Command(UiCommand::LoadTasks);
        }
        KeyCode::F(3) => {
            state.tab = Tab::Members;
            state.focus = Focus::Main;
            return UiInput::Command(UiCommand::LoadMembers);
        }
        KeyCode::F(4) => return open_form(state, FormKind::Invite),
        KeyCode::Char('q') => return request_detach(state),
        KeyCode::Char('?') => {
            state.modal = Some(Modal::Help { scroll: 0 });
            return UiInput::None;
        }
        KeyCode::Tab | KeyCode::BackTab => {
            cycle_focus(
                state,
                key.code == KeyCode::BackTab || key.modifiers.contains(KeyModifiers::SHIFT),
            );
            return UiInput::None;
        }
        KeyCode::Esc => {
            state.focus = Focus::Main;
            return UiInput::None;
        }
        _ => {}
    }
    if state.focus == Focus::Workspaces {
        return workspace_key(state, key);
    }
    if key.code == KeyCode::Enter {
        return open_detail(state);
    }
    if state.focus == Focus::Detail {
        scroll_key(
            &mut state.detail_scroll,
            key,
            usize::from(state.height.saturating_sub(10)).max(1),
        );
        return UiInput::None;
    }
    match state.tab {
        Tab::Chat => chat_key(state, key),
        Tab::Tasks => task_key(state, key),
        Tab::Members => {
            if let Some(delta) = movement(key, state.height) {
                state.member_selected = state
                    .member_selected
                    .saturating_add_signed(delta)
                    .min(state.members.len().saturating_sub(1));
                state.detail_scroll = 0;
            }
            UiInput::None
        }
    }
}

fn cycle_focus(state: &mut UiState, backwards: bool) {
    let mut focuses = [
        Focus::Workspaces,
        Focus::Main,
        Focus::Composer,
        Focus::Detail,
    ];
    let mut count = 2;
    if state.tab == Tab::Chat {
        count += 1;
    }
    if state.width >= WIDE_WIDTH {
        focuses[count] = Focus::Detail;
        count += 1;
    }
    let current = focuses[..count]
        .iter()
        .position(|focus| *focus == state.focus)
        .unwrap_or(1);
    let next = if backwards {
        (current + count - 1) % count
    } else {
        (current + 1) % count
    };
    state.focus = focuses[next];
}

fn movement(key: KeyEvent, height: u16) -> Option<isize> {
    let page = isize::try_from(height.saturating_sub(10).max(1)).unwrap_or(1);
    match key.code {
        KeyCode::Up => Some(-1),
        KeyCode::Down => Some(1),
        KeyCode::PageUp => Some(-page),
        KeyCode::PageDown => Some(page),
        KeyCode::Home => Some(isize::MIN),
        KeyCode::End => Some(isize::MAX),
        _ => None,
    }
}

fn scroll_key(scroll: &mut usize, key: KeyEvent, page: usize) {
    match key.code {
        KeyCode::Up => *scroll = scroll.saturating_sub(1),
        KeyCode::Down => *scroll = scroll.saturating_add(1),
        KeyCode::PageUp => *scroll = scroll.saturating_sub(page),
        KeyCode::PageDown => *scroll = scroll.saturating_add(page),
        KeyCode::Home => *scroll = 0,
        _ => {}
    }
}

fn workspace_key(state: &mut UiState, key: KeyEvent) -> UiInput {
    match key.code {
        KeyCode::Char('[') if state.workspaces.pagination.previous_page() => {
            UiInput::Command(UiCommand::LoadWorkspaces)
        }
        KeyCode::Char(']') if state.workspaces.pagination.next_page() => {
            UiInput::Command(UiCommand::LoadWorkspaces)
        }
        KeyCode::Enter => {
            let Some(row) = state.workspaces.rows.get(state.workspaces.selected) else {
                return UiInput::None;
            };
            if state.workspace.as_ref() == Some(&row.name) {
                return UiInput::None;
            }
            if state.form_busy
                || state.mutation.is_some()
                || state.composer.is_locked()
                || !state.composer.input.text().is_empty()
                || state.detach_needs_confirmation()
                || state.switching.is_some()
            {
                return fail(state, StateError::Busy);
            }
            if !matches!(state.connection, Connection::Connected { .. }) {
                return fail(state, StateError::NotConnected);
            }
            UiInput::Command(UiCommand::SelectWorkspace(row.name.clone()))
        }
        _ => {
            if let Some(delta) = movement(key, state.height) {
                state.workspaces.selected = state
                    .workspaces
                    .selected
                    .saturating_add_signed(delta)
                    .min(state.workspaces.rows.len().saturating_sub(1));
            }
            UiInput::None
        }
    }
}

fn chat_key(state: &mut UiState, key: KeyEvent) -> UiInput {
    match key.code {
        KeyCode::Char('i') => {
            state.focus = Focus::Composer;
            UiInput::None
        }
        KeyCode::Char('e') => {
            state.chat.all_events = !state.chat.all_events;
            state.chat.select_relative(0);
            UiInput::None
        }
        KeyCode::End => {
            state.chat.begin_return_to_live();
            UiInput::Command(UiCommand::RecentHistory)
        }
        KeyCode::Char('[') | KeyCode::PageUp => previous_history(state),
        KeyCode::Char(']') | KeyCode::PageDown if state.chat.mode == HistoryMode::Past => {
            if state.chat.display.has_more {
                UiInput::Command(UiCommand::NextHistory)
            } else {
                UiInput::None
            }
        }
        KeyCode::Up => {
            let first = state
                .chat
                .buffer
                .events()
                .iter()
                .find(|event| state.chat.visible(event))
                .map(|event| event.seq);
            if first == state.chat.selected_seq {
                previous_history(state)
            } else {
                state.chat.select_relative(-1);
                UiInput::None
            }
        }
        KeyCode::Down => {
            state.chat.select_relative(1);
            UiInput::None
        }
        KeyCode::PageDown => {
            state
                .chat
                .select_relative(isize::try_from(state.height.saturating_sub(10)).unwrap_or(1));
            UiInput::None
        }
        KeyCode::Home => {
            state.chat.select_relative(isize::MIN);
            UiInput::None
        }
        _ => UiInput::None,
    }
}

fn previous_history(state: &mut UiState) -> UiInput {
    if state
        .chat
        .buffer
        .events()
        .front()
        .is_none_or(|event| event.seq <= 1)
    {
        state.notice = Some(Notice {
            message: "Beginning of workspace history.",
            error: None,
        });
        UiInput::None
    } else {
        state.chat.follow = false;
        UiInput::Command(UiCommand::PreviousHistory)
    }
}

fn task_key(state: &mut UiState, key: KeyEvent) -> UiInput {
    let form = match key.code {
        KeyCode::Char('a') => Some(FormKind::TaskAssign),
        KeyCode::Char('r') => Some(FormKind::TaskRequest),
        KeyCode::Char('i') => Some(FormKind::TaskInterrupt),
        KeyCode::Char('f') => Some(FormKind::TaskConfirmStopped),
        KeyCode::Char('e') => Some(FormKind::TaskEdit),
        KeyCode::Char('m') => Some(FormKind::TaskNote),
        KeyCode::Char('c') => Some(FormKind::TaskCancel),
        KeyCode::Char('o') => Some(FormKind::TaskReopen),
        KeyCode::Char('/') => Some(FormKind::TaskFilter),
        _ => None,
    };
    if let Some(kind) = form {
        return open_form(state, kind);
    }
    match key.code {
        KeyCode::Char('[') if state.tasks.pagination.previous_page() => {
            UiInput::Command(UiCommand::LoadTasks)
        }
        KeyCode::Char(']') if state.tasks.pagination.next_page() => {
            UiInput::Command(UiCommand::LoadTasks)
        }
        KeyCode::Char('h') if state.tasks.selected_id.is_some() => {
            state.modal = Some(Modal::Detail {
                kind: DetailKind::TaskHistory,
                scroll: 0,
            });
            UiInput::Command(UiCommand::LoadTaskHistory)
        }
        _ => {
            if let Some(delta) = movement(key, state.height) {
                state.tasks.select_relative(delta);
                state.detail_scroll = 0;
                if state.tasks.selected_id.is_some() {
                    return UiInput::Command(UiCommand::LoadTaskDetail);
                }
            }
            UiInput::None
        }
    }
}

fn open_detail(state: &mut UiState) -> UiInput {
    let kind = match state.tab {
        Tab::Chat => state.chat.selected_seq.map(DetailKind::Event),
        Tab::Tasks => state.tasks.selected_id.map(|_| DetailKind::Task),
        Tab::Members => state
            .members
            .get(state.member_selected)
            .map(|_| DetailKind::Member(state.member_selected)),
    };
    let Some(kind) = kind else {
        return UiInput::None;
    };
    state.modal = Some(Modal::Detail { kind, scroll: 0 });
    if kind == DetailKind::Task {
        UiInput::Command(UiCommand::LoadTaskDetail)
    } else {
        UiInput::None
    }
}
