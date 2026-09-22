use std::sync::Mutex;

use crate::protocol::{TaskDispatch, TaskFence, WorkspaceName};

use super::{CancelReason, ProviderError, SessionRequest, TerminalEvidence};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InboxPhase {
    Idle,
    Running,
    Cancelling,
    StopPending,
    Closed,
}

struct Active {
    workspace: WorkspaceName,
    request_id: String,
    task: Option<TaskDispatch>,
    fence: Option<TaskFence>,
    result_settled: bool,
    cancel_reason: Option<CancelReason>,
}

struct PendingStop {
    active: Active,
    evidence: TerminalEvidence,
    action_issued: bool,
}

enum State {
    Idle,
    Active(Active),
    StopPending(PendingStop),
    Closed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboxTerminalAction {
    pub workspace: WorkspaceName,
    pub evidence: TerminalEvidence,
    pub fence: Option<TaskFence>,
    pub cancelled: bool,
}

pub struct ProviderInbox {
    state: Mutex<State>,
}

impl Default for ProviderInbox {
    fn default() -> Self {
        Self::new()
    }
}

impl ProviderInbox {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: Mutex::new(State::Idle),
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[must_use]
    pub fn is_idle(&self) -> bool {
        let state = self.lock_state();
        matches!(&*state, State::Idle)
    }
    #[must_use]
    pub fn phase(&self) -> InboxPhase {
        let state = self.lock_state();
        match &*state {
            State::Idle => InboxPhase::Idle,
            State::Active(active) if active.cancel_reason.is_some() => InboxPhase::Cancelling,
            State::Active(_) => InboxPhase::Running,
            State::StopPending(_) => InboxPhase::StopPending,
            State::Closed => InboxPhase::Closed,
        }
    }

    pub fn begin(&self, request: &SessionRequest) -> Result<(), ProviderError> {
        let workspace = request
            .workspace
            .clone()
            .ok_or(ProviderError::ExecutionFenceChanged)?;
        let mut state = self.lock_state();
        match &*state {
            State::Idle => {
                *state = State::Active(Active {
                    workspace,
                    request_id: request.request_id.clone(),
                    task: request.task.clone(),
                    fence: None,
                    result_settled: false,
                    cancel_reason: None,
                });
                Ok(())
            }
            State::Closed => Err(ProviderError::ProviderNotReady),
            State::Active(_) | State::StopPending(_) => Err(ProviderError::SessionBusy),
        }
    }

    pub fn bind_task_fence(
        &self,
        request_id: &str,
        fence: TaskFence,
    ) -> Result<Option<InboxTerminalAction>, ProviderError> {
        let mut state = self.lock_state();
        match &mut *state {
            State::Active(active) => {
                validate_fence(active, request_id, &fence)?;
                active.fence = Some(fence);
                Ok(None)
            }
            State::StopPending(pending) => {
                validate_fence(&pending.active, request_id, &fence)?;
                pending.active.fence = Some(fence);
                Ok(take_terminal_action(pending))
            }
            State::Idle | State::Closed => Err(ProviderError::ProviderNotReady),
        }
    }

    pub fn result_settled(&self, request_id: &str) -> bool {
        let mut state = self.lock_state();
        let active = match &mut *state {
            State::Active(active) => active,
            State::StopPending(pending) => &mut pending.active,
            State::Idle | State::Closed => return false,
        };
        if active.request_id != request_id || active.result_settled {
            return false;
        }
        active.result_settled = true;
        true
    }

    pub fn request_cancel(
        &self,
        workspace: &WorkspaceName,
        request_id: &str,
        fence: Option<&TaskFence>,
        reason: CancelReason,
    ) -> Result<bool, ProviderError> {
        let mut state = self.lock_state();
        let active = match &mut *state {
            State::Active(active) => active,
            State::StopPending(_) => return Ok(false),
            State::Idle | State::Closed => return Err(ProviderError::ProviderNotReady),
        };
        if active.workspace != *workspace || active.request_id != request_id {
            return Err(ProviderError::ExecutionFenceChanged);
        }
        validate_cancel_fence(active, fence)?;
        if active.cancel_reason.is_some() {
            return Ok(false);
        }
        active.cancel_reason = Some(reason);
        Ok(true)
    }

    #[must_use]
    pub fn acknowledge_cancel(&self, request_id: &str) -> bool {
        let state = self.lock_state();
        match &*state {
            State::Active(active) => {
                active.request_id == request_id && active.cancel_reason.is_some()
            }
            State::Idle | State::StopPending(_) | State::Closed => false,
        }
    }

    #[must_use]
    pub fn observe_terminal(&self, evidence: TerminalEvidence) -> Option<InboxTerminalAction> {
        let mut state = self.lock_state();
        let State::Active(active) = &*state else {
            return None;
        };
        if active.request_id != evidence.request_id {
            return None;
        }
        let State::Active(active) = std::mem::replace(&mut *state, State::Idle) else {
            unreachable!();
        };
        let mut pending = PendingStop {
            active,
            evidence,
            action_issued: false,
        };
        let action = take_terminal_action(&mut pending);
        *state = State::StopPending(pending);
        action
    }

    pub fn confirm_stopped(
        &self,
        workspace: &WorkspaceName,
        request_id: &str,
        fence: Option<&TaskFence>,
    ) -> bool {
        let mut state = self.lock_state();
        let State::StopPending(pending) = &*state else {
            return false;
        };
        if pending.active.workspace != *workspace
            || pending.active.request_id != request_id
            || pending.active.fence.as_ref() != fence
            || (pending.active.task.is_some() && fence.is_none())
            || !pending.action_issued
        {
            return false;
        }
        *state = State::Idle;
        true
    }

    pub fn close(&self) -> Result<(), ProviderError> {
        let mut state = self.lock_state();
        match &*state {
            State::Idle => {
                *state = State::Closed;
                Ok(())
            }
            State::Closed => Ok(()),
            State::Active(_) | State::StopPending(_) => Err(ProviderError::SessionBusy),
        }
    }
}

fn validate_fence(
    active: &Active,
    request_id: &str,
    fence: &TaskFence,
) -> Result<(), ProviderError> {
    if active.request_id != request_id
        || active.task.as_ref().map(|task| task.id) != Some(fence.task_id)
        || active
            .fence
            .as_ref()
            .is_some_and(|current| current != fence)
    {
        return Err(ProviderError::ExecutionFenceChanged);
    }
    Ok(())
}

fn validate_cancel_fence(active: &Active, fence: Option<&TaskFence>) -> Result<(), ProviderError> {
    let matches = match (&active.task, &active.fence, fence) {
        (Some(task), Some(bound), Some(cancelled)) => {
            task.id == cancelled.task_id && bound == cancelled
        }
        (None, None, None) => true,
        (Some(_), None, _) | (Some(_), Some(_), None) | (None, _, _) => false,
    };
    if matches {
        Ok(())
    } else {
        Err(ProviderError::ExecutionFenceChanged)
    }
}

fn take_terminal_action(pending: &mut PendingStop) -> Option<InboxTerminalAction> {
    if pending.action_issued || (pending.active.task.is_some() && pending.active.fence.is_none()) {
        None
    } else {
        pending.action_issued = true;
        Some(InboxTerminalAction {
            workspace: pending.active.workspace.clone(),
            evidence: pending.evidence.clone(),
            fence: pending.active.fence.clone(),
            cancelled: pending.active.cancel_reason.is_some(),
        })
    }
}
