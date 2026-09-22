use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::broadcast;
use tokio::time::{Instant, timeout};

use crate::{
    client::{ExecutionBarrier, RouterClient},
    protocol::{TaskDispatch, TaskExecutionEvidence, TaskFence, WorkspaceName},
    tasks::PauseReason,
};

use super::{
    CancelReason, OwnedProvider, ProviderError, RouterProviderLifecycle, SessionRequest,
    SessionResult, TerminalEvidence, TerminalReason,
    inbox::{InboxTerminalAction, ProviderInbox},
};

const TERMINAL_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(6);

#[derive(Clone, Debug, Eq, PartialEq)]
struct BarrierLease {
    session_id: uuid::Uuid,
    task: Option<TaskFence>,
}

/// Binds provider lifecycle evidence to one router connection and workspace.
///
/// The adapter deliberately reads the client's applied attempt state twice: once before the
/// provider owns a turn and once after provider-terminal evidence. A reconnect, a different task
/// fence, or unavailable attempt state leaves the execution barrier unresolved.
#[derive(Clone)]
pub struct RouterClientLifecycle {
    client: RouterClient,
    workspace: WorkspaceName,
    lease: Arc<Mutex<Option<BarrierLease>>>,
    explicit_readiness: bool,
}

impl RouterClientLifecycle {
    #[must_use]
    pub fn new(client: RouterClient, workspace: WorkspaceName) -> Self {
        Self::new_with_explicit_readiness(client, workspace, true)
    }

    #[must_use]
    pub fn new_with_explicit_readiness(
        client: RouterClient,
        workspace: WorkspaceName,
        explicit_readiness: bool,
    ) -> Self {
        Self {
            client,
            workspace,
            lease: Arc::new(Mutex::new(None)),
            explicit_readiness,
        }
    }

    #[must_use]
    pub fn workspace(&self) -> &WorkspaceName {
        &self.workspace
    }

    async fn acquire_barrier(&self) -> Result<Option<TaskFence>, ProviderError> {
        if self
            .lease
            .lock()
            .expect("provider lifecycle mutex poisoned")
            .is_some()
        {
            return Err(ProviderError::SessionBusy);
        }
        let session_id = self
            .client
            .session_id()
            .ok_or(ProviderError::RouterLifecycleUnavailable)?;
        let barrier = self.read_barrier().await?;
        if barrier.session_id != session_id || barrier.stop_pending.is_some() {
            return Err(ProviderError::ExecutionFenceChanged);
        }
        let task = barrier.current;
        let mut lease = self
            .lease
            .lock()
            .expect("provider lifecycle mutex poisoned");
        if lease.is_some() {
            return Err(ProviderError::SessionBusy);
        }
        *lease = Some(BarrierLease {
            session_id,
            task: task.clone(),
        });
        Ok(task)
    }

    fn abandon_unowned_barrier(&self, task: Option<&TaskFence>) {
        let mut lease = self
            .lease
            .lock()
            .expect("provider lifecycle mutex poisoned");
        if lease
            .as_ref()
            .is_some_and(|lease| lease.task.as_ref() == task)
        {
            *lease = None;
        }
    }

    async fn read_barrier(&self) -> Result<ExecutionBarrier, ProviderError> {
        self.client
            .execution_barrier()
            .await
            .map_err(|_| ProviderError::RouterLifecycleUnavailable)
    }

    async fn finish_execution(
        &self,
        evidence: TerminalEvidence,
        task: Option<TaskFence>,
    ) -> Result<(), ProviderError> {
        let lease = self
            .lease
            .lock()
            .expect("provider lifecycle mutex poisoned")
            .clone()
            .ok_or(ProviderError::ExecutionFenceChanged)?;
        if lease.task != task {
            return Err(ProviderError::ExecutionFenceChanged);
        }

        let session_id = self
            .client
            .session_id()
            .ok_or(ProviderError::RouterLifecycleUnavailable)?;
        let barrier = self.read_barrier().await?;
        if session_id != lease.session_id || barrier.session_id != lease.session_id {
            return Err(ProviderError::ExecutionFenceChanged);
        }

        match task.as_ref() {
            Some(fence)
                if barrier.current.as_ref() == Some(fence)
                    || barrier.stop_pending.as_ref() == Some(fence) =>
            {
                if barrier
                    .current
                    .as_ref()
                    .zip(barrier.stop_pending.as_ref())
                    .is_some_and(|(current, pending)| current != pending)
                {
                    return Err(ProviderError::ExecutionFenceChanged);
                }
                self.client
                    .task_execution_stopped(
                        self.workspace.clone(),
                        fence.task_id,
                        fence.attempt_id,
                        lease.session_id,
                        if evidence.child_reaped {
                            TaskExecutionEvidence::ProviderClosed
                        } else {
                            TaskExecutionEvidence::ProviderTerminal
                        },
                        pause_reason(evidence.reason),
                    )
                    .await
                    .map_err(|_| ProviderError::RouterLifecycleUnavailable)?;
            }
            Some(_) | None if barrier.current.is_none() && barrier.stop_pending.is_none() => {}
            Some(_) | None => return Err(ProviderError::ExecutionFenceChanged),
        }

        let mut current = self
            .lease
            .lock()
            .expect("provider lifecycle mutex poisoned");
        if current.as_ref() != Some(&lease) {
            return Err(ProviderError::ExecutionFenceChanged);
        }
        *current = None;
        Ok(())
    }

    async fn work_idle(
        &self,
        action: &InboxTerminalAction,
        ready: bool,
    ) -> Result<(), ProviderError> {
        self.client
            .work_idle(
                action.workspace.clone(),
                action.evidence.request_id.clone(),
                action.fence.clone(),
                ready,
            )
            .await
            .map_err(|_| ProviderError::RouterLifecycleUnavailable)
    }
}

impl RouterProviderLifecycle for RouterClientLifecycle {
    async fn set_ready(&self, ready: bool) -> Result<(), ProviderError> {
        if !self.explicit_readiness {
            return Ok(());
        }
        self.client
            .set_ready(ready)
            .await
            .map_err(|_| ProviderError::RouterLifecycleUnavailable)
    }

    fn execution_barrier(
        &self,
    ) -> impl Future<Output = Result<Option<TaskFence>, ProviderError>> + Send {
        self.acquire_barrier()
    }

    fn execution_stopped(
        &self,
        evidence: TerminalEvidence,
        fence: Option<TaskFence>,
    ) -> impl Future<Output = Result<(), ProviderError>> + Send {
        self.finish_execution(evidence, fence)
    }
}

/// Owns the router fences around one single-flight provider.
///
/// A provider result is not a stop signal. `handle` waits for the provider's separately observed
/// terminal event, re-reads the applied router barrier, and only then confirms an exact task stop.
pub struct RouterManagedProvider<P> {
    provider: P,
    lifecycle: RouterClientLifecycle,
    inbox: ProviderInbox,
}

impl<P: OwnedProvider> RouterManagedProvider<P> {
    #[must_use]
    pub fn new(provider: P, lifecycle: RouterClientLifecycle) -> Self {
        Self {
            provider,
            lifecycle,
            inbox: ProviderInbox::new(),
        }
    }

    pub async fn activate(&self) -> Result<(), ProviderError> {
        self.lifecycle.set_ready(self.provider.ready()).await
    }

    #[must_use]
    pub fn ready(&self) -> bool {
        self.provider.ready() && self.inbox.is_idle()
    }

    pub async fn handle(&self, request: SessionRequest) -> SessionResult {
        if request.workspace.as_ref() != Some(self.lifecycle.workspace()) {
            return SessionResult::failure(ProviderError::ExecutionFenceChanged);
        }
        if !self.provider.ready() || request.deadline <= Instant::now() {
            return SessionResult::failure(if request.deadline <= Instant::now() {
                ProviderError::RequestTimeout
            } else {
                ProviderError::SessionBusy
            });
        }
        if let Err(error) = self.lifecycle.set_ready(false).await {
            return SessionResult::failure(error);
        }

        let fence = match self.lifecycle.execution_barrier().await {
            Ok(fence) => fence,
            Err(error) => return SessionResult::failure(error),
        };
        if !matching_dispatch(request.task.as_ref(), fence.as_ref())
            || request.deadline <= Instant::now()
        {
            self.lifecycle.abandon_unowned_barrier(fence.as_ref());
            let _ = self.lifecycle.set_ready(self.provider.ready()).await;
            return SessionResult::failure(if request.deadline <= Instant::now() {
                ProviderError::RequestTimeout
            } else {
                ProviderError::ExecutionFenceChanged
            });
        }
        if let Err(error) = self.inbox.begin(&request) {
            self.lifecycle.abandon_unowned_barrier(fence.as_ref());
            let _ = self.lifecycle.set_ready(self.provider.ready()).await;
            return SessionResult::failure(error);
        }
        if let Some(bound) = fence.clone()
            && let Err(error) = self.inbox.bind_task_fence(&request.request_id, bound)
        {
            self.lifecycle.abandon_unowned_barrier(fence.as_ref());
            return SessionResult::failure(error);
        }

        let mut terminal = self.provider.subscribe_terminal();
        let result = self.provider.handle(request.clone()).await;
        self.inbox.result_settled(&request.request_id);
        let evidence = match wait_for_terminal(&mut terminal, &request.request_id).await {
            Ok(evidence) => evidence,
            Err(error) => return SessionResult::failure(error),
        };
        let Some(action) = self.inbox.observe_terminal(evidence) else {
            return SessionResult::failure(ProviderError::ExecutionFenceChanged);
        };
        if let Err(error) = self
            .lifecycle
            .execution_stopped(action.evidence.clone(), action.fence.clone())
            .await
        {
            return SessionResult::failure(error);
        }

        let ready = self.provider.ready();
        if action.cancelled {
            if let Err(error) = self.lifecycle.work_idle(&action, ready).await {
                return SessionResult::failure(error);
            }
        } else if let Err(error) = self.lifecycle.set_ready(ready).await {
            return SessionResult::failure(error);
        }
        if self.inbox.confirm_stopped(
            &action.workspace,
            &action.evidence.request_id,
            action.fence.as_ref(),
        ) {
            result
        } else {
            SessionResult::failure(ProviderError::ExecutionFenceChanged)
        }
    }

    pub async fn cancel(
        &self,
        workspace: &WorkspaceName,
        request_id: &str,
        task: Option<&TaskFence>,
        reason: CancelReason,
    ) -> Result<(), ProviderError> {
        if !self
            .inbox
            .request_cancel(workspace, request_id, task, reason)?
        {
            return Ok(());
        }
        self.provider.cancel(request_id.to_owned(), reason).await
    }

    pub async fn close(&self) -> Result<(), ProviderError> {
        self.lifecycle.set_ready(false).await?;
        self.provider.close().await
    }
}

fn matching_dispatch(dispatch: Option<&TaskDispatch>, fence: Option<&TaskFence>) -> bool {
    match (dispatch, fence) {
        (Some(dispatch), Some(fence)) => dispatch.id == fence.task_id,
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}

async fn wait_for_terminal(
    terminal: &mut broadcast::Receiver<TerminalEvidence>,
    request_id: &str,
) -> Result<TerminalEvidence, ProviderError> {
    timeout(TERMINAL_OBSERVATION_TIMEOUT, async {
        loop {
            match terminal.recv().await {
                Ok(evidence) if evidence.request_id == request_id => return Ok(evidence),
                Ok(_) => {}
                Err(
                    broadcast::error::RecvError::Lagged(_) | broadcast::error::RecvError::Closed,
                ) => return Err(ProviderError::RouterLifecycleUnavailable),
            }
        }
    })
    .await
    .map_err(|_| ProviderError::RouterLifecycleUnavailable)?
}

const fn pause_reason(reason: TerminalReason) -> PauseReason {
    match reason {
        TerminalReason::TurnEnded => PauseReason::TurnEnded,
        TerminalReason::SessionEnded => PauseReason::SessionEnded,
        TerminalReason::HostError => PauseReason::HostError,
        TerminalReason::OperatorInterrupt => PauseReason::OperatorInterrupt,
        TerminalReason::RequestTimeout => PauseReason::RequestTimeout,
        TerminalReason::RequestCancelled => PauseReason::RequestCancelled,
    }
}
