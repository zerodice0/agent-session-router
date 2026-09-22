use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    io,
    net::{IpAddr, SocketAddr, TcpListener, ToSocketAddrs},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    task::{Context, Poll},
    thread,
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    extract::{
        ConnectInfo, Path, Request, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, Utf8Bytes, WebSocket},
    },
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::{SinkExt, StreamExt};
use hyper_util::rt::TokioTimer;
use serde::Serialize;
use tokio::time::Instant;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::{
    bootstrap::{BootstrapAssets, BootstrapError},
    credentials::{
        CredentialFile, CredentialRole, PublicCredentialClaims, hash_token, read_credential,
        write_credential_atomic_no_replace,
    },
    integrations::{
        ExternalCommand, ExternalErrorCode, IntegrationClient, IntegrationClientError,
        IntegrationRegistry, OperationPayload, PreparedExternalOperation,
        PreparedExternalResolution, ResolutionPreparation, complete_applied_resolution,
        complete_external_error, complete_external_mutation, complete_external_read,
        external_status as load_external_status, mark_external_dispatched,
        prepare_external_operation, prepare_external_resolution, replay_external_operation,
        terminal_external_error, validate_external_snapshot, validate_resolution_snapshot,
    },
    onboarding::{
        EnrollmentError, EnrollmentRequest, EnrollmentResponse, MAX_ENROLLMENT_BYTES,
        OnboardingInfo, VERSION as ONBOARDING_VERSION,
    },
    protocol::{
        AgentDescriptor, AgentRegistration, AgentStatus, ClientMessage, CredentialSummary,
        DeliveryMode, HistoryPage, PROTOCOL_VERSION, RegistrationRole, RouterErrorCode,
        ServerMessage, TaskDispatch, TaskExecutionEvidence, TaskFence, TaskHistoryEvent,
        TaskHistoryPage, WorkspaceEvent, WorkspaceEventKind, WorkspaceName, WorkspaceSummary,
        normalize_timeout_ms, parse_client_message, validate_page,
    },
    store::{EventInsert, RouterStore, StoreError},
    tasks::{
        CallerContext, CallerRole, ExternalProvider, ExternalResolutionOutcome, PauseReason,
        ReservationFence, TaskAttempt, TaskCommand, TaskEvent, TaskState, TaskSummary,
        agent_execution_barriers, agent_execution_fences, agent_has_execution_barrier, apply_task,
        attempt_agent_id, execution_attempt, get_task, interrupt_attempt,
        interrupt_session_attempts, list_tasks, record_execution_stopped,
    },
    tls::{install_crypto_provider, load_server_config},
};

pub const ACTOR_COMMAND_CAPACITY: usize = 1024;
pub const ACTOR_BYTE_CAPACITY: usize = 16 * 1024 * 1024;
pub const CONTROL_CAPACITY: usize = 512;
pub const CONNECTION_LIMIT: usize = 256;
pub const DIRECT_PREAUTH_PER_IP_LIMIT: usize = 8;
pub const TAILSCALE_SERVE_PREAUTH_LIMIT: usize = 32;
pub const WRITER_FRAME_CAPACITY: usize = 64;
pub const WRITER_BYTE_CAPACITY: usize = 1024 * 1024;
pub const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(5);
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
pub const PENDING_REQUEST_LIMIT: usize = 256;
pub const INTEGRATION_REQUEST_LIMIT: usize = 4;
pub const CREDENTIAL_MESSAGES_PER_SECOND: u64 = 20;
pub const CREDENTIAL_MESSAGE_BURST: u64 = 40;
pub const ENROLLMENT_IN_FLIGHT_LIMIT: usize = 8;
pub const ENROLLMENT_GLOBAL_PER_MINUTE: usize = 60;
pub const ENROLLMENT_PEER_PER_MINUTE: usize = 10;
pub const ENROLLMENT_PEER_LIMIT: usize = 1024;
const ENROLLMENT_WINDOW: Duration = Duration::from_secs(60);
const ENROLLMENT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouterExposure {
    Direct,
    TailscaleServe,
}

#[derive(Clone, Debug)]
pub struct RouterConfig {
    pub bind: SocketAddr,
    pub data_dir: PathBuf,
    pub instance_id: Uuid,
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
    pub public_url: Option<url::Url>,
    pub exposure: RouterExposure,
    pub onboarding_assets_dir: Option<PathBuf>,
}

#[derive(Clone)]
struct BoundedAcceptor<A> {
    inner: A,
    slots: Arc<Semaphore>,
}

impl<I, S, A> axum_server::accept::Accept<I, S> for BoundedAcceptor<A>
where
    I: Send + 'static,
    S: Send + 'static,
    A: axum_server::accept::Accept<I, S>,
    A::Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    A::Service: Send + 'static,
    A::Future: Send + 'static,
{
    type Stream = ConnectionStream<A::Stream>;
    type Service = A::Service;
    type Future =
        Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Self::Service)>> + Send + 'static>>;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        let slot = self.slots.clone().try_acquire_owned();
        let accepted = self.inner.accept(stream, service);
        Box::pin(async move {
            let slot = slot.map_err(|_| {
                io::Error::new(io::ErrorKind::ConnectionAborted, "connection limit reached")
            })?;
            let (stream, service) = tokio::time::timeout(HANDSHAKE_TIMEOUT, accepted)
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "handshake timed out"))??;
            Ok((
                ConnectionStream {
                    inner: stream,
                    _slot: slot,
                },
                service,
            ))
        })
    }
}

struct ConnectionStream<I> {
    inner: I,
    _slot: OwnedSemaphorePermit,
}

impl<I: AsyncRead + Unpin> AsyncRead for ConnectionStream<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for ConnectionStream<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.inner).poll_write_vectored(context, buffers)
    }
}

#[derive(Clone)]
enum PreAuthLimiter {
    Direct(Arc<Mutex<HashMap<IpAddr, usize>>>),
    TailscaleServe(Arc<Semaphore>),
}

impl PreAuthLimiter {
    fn new(exposure: RouterExposure) -> Self {
        match exposure {
            RouterExposure::Direct => Self::Direct(Arc::new(Mutex::new(HashMap::new()))),
            RouterExposure::TailscaleServe => {
                Self::TailscaleServe(Arc::new(Semaphore::new(TAILSCALE_SERVE_PREAUTH_LIMIT)))
            }
        }
    }

    fn try_acquire(&self, peer: IpAddr) -> Option<PreAuthPermit> {
        match self {
            Self::Direct(counts) => {
                let mut locked = counts.lock().ok()?;
                let count = locked.entry(peer).or_default();
                if *count >= DIRECT_PREAUTH_PER_IP_LIMIT {
                    return None;
                }
                *count += 1;
                Some(PreAuthPermit::Direct {
                    peer,
                    counts: counts.clone(),
                })
            }
            Self::TailscaleServe(slots) => slots
                .clone()
                .try_acquire_owned()
                .ok()
                .map(|permit| PreAuthPermit::TailscaleServe { _permit: permit }),
        }
    }
}

enum PreAuthPermit {
    Direct {
        peer: IpAddr,
        counts: Arc<Mutex<HashMap<IpAddr, usize>>>,
    },
    TailscaleServe {
        _permit: OwnedSemaphorePermit,
    },
}

impl Drop for PreAuthPermit {
    fn drop(&mut self) {
        if let Self::Direct { peer, counts } = self
            && let Ok(mut locked) = counts.lock()
            && let Some(count) = locked.get_mut(peer)
        {
            *count -= 1;
            if *count == 0 {
                locked.remove(peer);
            }
        }
    }
}

#[derive(Clone)]
struct EnrollmentLimiter {
    slots: Arc<Semaphore>,
    rate: Arc<Mutex<EnrollmentRate>>,
}

#[derive(Default)]
struct EnrollmentRate {
    recent: VecDeque<(Instant, IpAddr)>,
    peers: HashMap<IpAddr, usize>,
}

impl EnrollmentLimiter {
    fn new() -> Self {
        Self {
            slots: Arc::new(Semaphore::new(ENROLLMENT_IN_FLIGHT_LIMIT)),
            rate: Arc::new(Mutex::new(EnrollmentRate::default())),
        }
    }

    fn try_acquire(&self, peer: IpAddr) -> Option<OwnedSemaphorePermit> {
        let slot = self.slots.clone().try_acquire_owned().ok()?;
        let mut rate = self.rate.lock().ok()?;
        rate.record(peer, Instant::now()).then_some(slot)
    }
}

impl EnrollmentRate {
    fn record(&mut self, peer: IpAddr, now: Instant) -> bool {
        while self
            .recent
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) >= ENROLLMENT_WINDOW)
        {
            let Some((_, expired_peer)) = self.recent.pop_front() else {
                return false;
            };
            if let Some(count) = self.peers.get_mut(&expired_peer) {
                *count -= 1;
                if *count == 0 {
                    self.peers.remove(&expired_peer);
                }
            }
        }
        if self.recent.len() >= ENROLLMENT_GLOBAL_PER_MINUTE
            || self.peers.get(&peer).copied().unwrap_or(0) >= ENROLLMENT_PEER_PER_MINUTE
            || (!self.peers.contains_key(&peer) && self.peers.len() >= ENROLLMENT_PEER_LIMIT)
        {
            return false;
        }
        self.recent.push_back((now, peer));
        *self.peers.entry(peer).or_default() += 1;
        true
    }
}

#[cfg(test)]
mod enrollment_rate_tests {
    use super::*;

    #[test]
    fn sliding_window_enforces_global_and_peer_limits_until_exact_expiration() {
        let mut rate = EnrollmentRate::default();
        let now = Instant::now();
        let peer = IpAddr::from([192, 0, 2, 1]);
        for _ in 0..ENROLLMENT_PEER_PER_MINUTE {
            assert!(rate.record(peer, now));
        }
        assert!(!rate.record(peer, now));
        for index in 2..=6 {
            for _ in 0..ENROLLMENT_PEER_PER_MINUTE {
                assert!(rate.record(IpAddr::from([192, 0, 2, index]), now));
            }
        }
        let another = IpAddr::from([192, 0, 2, 7]);
        assert!(!rate.record(another, now));
        assert!(!rate.record(another, now + ENROLLMENT_WINDOW - Duration::from_nanos(1)));
        assert!(rate.record(another, now + ENROLLMENT_WINDOW));
        for _ in 0..ENROLLMENT_PEER_PER_MINUTE {
            assert!(rate.record(peer, now + ENROLLMENT_WINDOW));
        }
        assert!(!rate.record(peer, now + ENROLLMENT_WINDOW));
    }
}

pub struct RouterRuntime {
    pub address: SocketAddr,
    pub instance_id: Uuid,
    handle: RouterHandle,
    startup: Option<oneshot::Sender<()>>,
    completion: tokio::task::JoinHandle<Result<(), RouterRuntimeError>>,
}

impl RouterRuntime {
    pub async fn start(config: RouterConfig) -> Result<Self, RouterRuntimeError> {
        Self::start_inner(config, false).await
    }

    pub async fn start_paused(config: RouterConfig) -> Result<Self, RouterRuntimeError> {
        Self::start_inner(config, true).await
    }

    async fn start_inner(config: RouterConfig, paused: bool) -> Result<Self, RouterRuntimeError> {
        install_crypto_provider().map_err(|_| RouterRuntimeError::Configuration)?;
        let tls_config = validate_transport_config(&config)?;
        let onboarding_assets = match config.onboarding_assets_dir.as_deref() {
            Some(directory) => BootstrapAssets::load(directory)?.map(Arc::new),
            None => None,
        };
        let listener = TcpListener::bind(config.bind).map_err(RouterRuntimeError::Io)?;
        listener
            .set_nonblocking(true)
            .map_err(RouterRuntimeError::Io)?;
        let address = listener.local_addr().map_err(RouterRuntimeError::Io)?;
        if let Some(public_url) = config.public_url.as_ref() {
            validate_public_endpoint(public_url, address)?;
        }

        let (ready_tx, ready_rx) = oneshot::channel();
        let actor_config = config.clone();
        let actor_thread = thread::Builder::new()
            .name("asr-router-state".to_owned())
            .spawn(move || run_actor_thread(actor_config, ready_tx))
            .map_err(RouterRuntimeError::Io)?;
        let handle = ready_rx
            .await
            .map_err(|_| RouterRuntimeError::ActorStopped)??;
        let connection_slots = Arc::new(Semaphore::new(CONNECTION_LIMIT));
        let app_state = AppState {
            handle: handle.clone(),
            pre_auth: PreAuthLimiter::new(config.exposure),
            next_generation: Arc::new(AtomicI64::new(1)),
            instance_id: config.instance_id,
            onboarding_assets,
            enrollment_limiter: EnrollmentLimiter::new(),
        };
        let onboarding = Router::new()
            .route("/info", get(onboarding_info))
            .route("/files/{name}", get(onboarding_file))
            .route("/enroll", post(onboarding_enroll))
            .fallback(onboarding_malformed)
            .method_not_allowed_fallback(onboarding_malformed)
            .layer(middleware::from_fn(onboarding_no_store));
        let app = Router::new()
            .route("/healthz", get(health))
            .route("/ws", get(websocket_upgrade))
            .nest("/onboarding", onboarding)
            .with_state(app_state);
        let shutdown_handle = handle.clone();
        let completion_handle = handle.clone();
        let server_handle = axum_server::Handle::new();
        let graceful_handle = server_handle.clone();
        let (startup, startup_receiver) = if paused {
            let (sender, receiver) = oneshot::channel();
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        let completion = tokio::spawn(async move {
            let shutdown_watcher = tokio::spawn(async move {
                shutdown_handle.wait_for_shutdown().await;
                graceful_handle.graceful_shutdown(Some(SHUTDOWN_TIMEOUT));
            });
            if let Some(startup_receiver) = startup_receiver
                && startup_receiver.await.is_err()
            {
                shutdown_watcher.abort();
                let _ = completion_handle.shutdown().await;
                return tokio::task::spawn_blocking(move || actor_thread.join())
                    .await
                    .map_err(|_| RouterRuntimeError::ActorStopped)?
                    .map_err(|_| RouterRuntimeError::ActorStopped)?;
            }
            let service = app.into_make_service_with_connect_info::<SocketAddr>();
            let server_result = match tls_config {
                Some(tls_config) => match axum_server::from_tcp_rustls(
                    listener,
                    axum_server::tls_rustls::RustlsConfig::from_config(tls_config),
                ) {
                    Ok(server) => {
                        let mut server = server.map(|inner| BoundedAcceptor {
                            inner,
                            slots: connection_slots.clone(),
                        });
                        server
                            .http_builder()
                            .http1()
                            .timer(TokioTimer::new())
                            .header_read_timeout(HANDSHAKE_TIMEOUT);
                        server
                            .handle(server_handle)
                            .serve(service)
                            .await
                            .map_err(RouterRuntimeError::Io)
                    }
                    Err(error) => Err(RouterRuntimeError::Io(error)),
                },
                None => match axum_server::from_tcp(listener) {
                    Ok(server) => {
                        let mut server = server.map(|inner| BoundedAcceptor {
                            inner,
                            slots: connection_slots,
                        });
                        server
                            .http_builder()
                            .http1()
                            .timer(TokioTimer::new())
                            .header_read_timeout(HANDSHAKE_TIMEOUT);
                        server
                            .handle(server_handle)
                            .serve(service)
                            .await
                            .map_err(RouterRuntimeError::Io)
                    }
                    Err(error) => Err(RouterRuntimeError::Io(error)),
                },
            };
            shutdown_watcher.abort();
            let _ = completion_handle.shutdown().await;
            let actor_result = tokio::task::spawn_blocking(move || actor_thread.join())
                .await
                .map_err(|_| RouterRuntimeError::ActorStopped)?
                .map_err(|_| RouterRuntimeError::ActorStopped)?;
            server_result?;
            actor_result
        });
        Ok(Self {
            address,
            instance_id: config.instance_id,
            handle,
            completion,
            startup,
        })
    }
    pub fn activate(&mut self) -> Result<(), RouterRuntimeError> {
        if let Some(startup) = self.startup.take() {
            startup
                .send(())
                .map_err(|()| RouterRuntimeError::ActorStopped)?;
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), RouterRuntimeError> {
        self.handle.shutdown().await
    }

    pub async fn revoke_credential(&self, id: Uuid) -> Result<bool, RouterRuntimeError> {
        self.handle.revoke_credential(id).await
    }

    pub async fn wait(mut self) -> Result<(), RouterRuntimeError> {
        self.startup.take();
        self.completion
            .await
            .map_err(|_| RouterRuntimeError::ActorStopped)?
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RouterRuntimeError {
    #[error("configuration_required")]
    Configuration,
    #[error("store_error")]
    Store(#[from] StoreError),
    #[error("credential_error")]
    Credential(#[from] crate::credentials::CredentialError),
    #[error("bootstrap_assets_invalid")]
    Bootstrap(#[from] BootstrapError),
    #[error("router_io")]
    Io(#[source] std::io::Error),
    #[error("router_actor_stopped")]
    ActorStopped,
}

fn validate_transport_config(
    config: &RouterConfig,
) -> Result<Option<Arc<rustls::ServerConfig>>, RouterRuntimeError> {
    match (
        config.tls_cert_file.as_deref(),
        config.tls_key_file.as_deref(),
        config.public_url.as_ref(),
    ) {
        (None, None, None) if config.bind.ip().is_loopback() => Ok(None),
        (Some(certificate), Some(private_key), Some(public_url)) => {
            if public_url.scheme() != "wss"
                || public_url.path() != "/ws"
                || public_url.query().is_some()
                || public_url.fragment().is_some()
                || !public_url.username().is_empty()
                || public_url.password().is_some()
                || public_url.host().is_none()
            {
                return Err(RouterRuntimeError::Configuration);
            }
            load_server_config(certificate, private_key)
                .map(Some)
                .map_err(|_| RouterRuntimeError::Configuration)
        }
        _ => Err(RouterRuntimeError::Configuration),
    }
}

fn validate_public_endpoint(
    public_url: &url::Url,
    address: SocketAddr,
) -> Result<(), RouterRuntimeError> {
    if public_url.port_or_known_default() != Some(address.port()) {
        return Err(RouterRuntimeError::Configuration);
    }
    let compatible_host = match public_url.host() {
        Some(url::Host::Ipv4(host)) => {
            address.ip().is_unspecified() || address.ip() == std::net::IpAddr::V4(host)
        }
        Some(url::Host::Ipv6(host)) => {
            address.ip().is_unspecified() || address.ip() == std::net::IpAddr::V6(host)
        }
        Some(url::Host::Domain(host)) if host.eq_ignore_ascii_case("localhost") => {
            address.ip().is_loopback()
        }
        Some(url::Host::Domain(host)) if address.ip().is_unspecified() => !host.is_empty(),
        Some(url::Host::Domain(host)) => (host, address.port())
            .to_socket_addrs()
            .is_ok_and(|mut addresses| addresses.any(|resolved| resolved.ip() == address.ip())),
        None => false,
    };
    if compatible_host {
        Ok(())
    } else {
        Err(RouterRuntimeError::Configuration)
    }
}

#[derive(Clone)]
struct AppState {
    handle: RouterHandle,
    pre_auth: PreAuthLimiter,
    next_generation: Arc<AtomicI64>,
    instance_id: Uuid,
    onboarding_assets: Option<Arc<BootstrapAssets>>,
    enrollment_limiter: EnrollmentLimiter,
}

#[derive(Serialize)]
struct OnboardingHttpError {
    code: &'static str,
}

fn onboarding_error(status: StatusCode, code: &'static str) -> Response {
    (status, Json(OnboardingHttpError { code })).into_response()
}

async fn onboarding_malformed() -> Response {
    onboarding_error(StatusCode::BAD_REQUEST, "malformed_request")
}

async fn onboarding_no_store(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn onboarding_info(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
) -> Response {
    let Some(_pre_auth) = state.pre_auth.try_acquire(peer.ip()) else {
        return onboarding_error(StatusCode::TOO_MANY_REQUESTS, "overloaded");
    };
    let (manifest_sha256, available_targets) = state.onboarding_assets.as_ref().map_or_else(
        || (None, Vec::new()),
        |assets| {
            (
                Some(assets.manifest_sha256().to_owned()),
                assets
                    .manifest()
                    .artifacts
                    .iter()
                    .map(|artifact| artifact.target.clone())
                    .collect(),
            )
        },
    );
    Json(OnboardingInfo {
        version: ONBOARDING_VERSION,
        server_id: state.handle.server_id,
        protocol_version: PROTOCOL_VERSION,
        manifest_sha256,
        available_targets,
    })
    .into_response()
}

async fn onboarding_file(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    name: Result<Path<String>, axum::extract::rejection::PathRejection>,
) -> Response {
    let Some(pre_auth) = state.pre_auth.try_acquire(peer.ip()) else {
        return onboarding_error(StatusCode::TOO_MANY_REQUESTS, "overloaded");
    };
    let Ok(Path(name)) = name else {
        return onboarding_malformed().await;
    };
    let Some(assets) = state.onboarding_assets else {
        return onboarding_error(StatusCode::SERVICE_UNAVAILABLE, "bootstrap_assets_missing");
    };
    let content_type = if name == "bootstrap-manifest.json" {
        "application/json"
    } else {
        "application/octet-stream"
    };
    let opened = tokio::task::spawn_blocking(move || assets.file(&name)).await;
    let (file, length) = match opened {
        Ok(Ok(Some(file))) => file,
        Ok(Ok(None)) => return onboarding_malformed().await,
        Ok(Err(_)) | Err(_) => {
            return onboarding_error(StatusCode::SERVICE_UNAVAILABLE, "bootstrap_assets_invalid");
        }
    };
    let stream =
        ReaderStream::with_capacity(tokio::fs::File::from_std(file).take(length), 64 * 1024).map(
            move |chunk| {
                // A slow download retains its pre-auth slot until the body is dropped.
                let _permit = &pre_auth;
                chunk
            },
        );
    (
        [
            (header::CONTENT_TYPE, content_type.to_owned()),
            (header::CONTENT_LENGTH, length.to_string()),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}

async fn enrollment_body(body: Body) -> Result<Vec<u8>, (StatusCode, &'static str)> {
    let mut stream = body.into_data_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| (StatusCode::BAD_REQUEST, "malformed_request"))?;
        if chunk.len() > MAX_ENROLLMENT_BYTES - bytes.len() {
            return Err((StatusCode::PAYLOAD_TOO_LARGE, "request_too_large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn onboarding_enroll(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    let Some(_pre_auth) = state.pre_auth.try_acquire(peer.ip()) else {
        return onboarding_error(StatusCode::TOO_MANY_REQUESTS, "overloaded");
    };
    let Some(slot) = state.enrollment_limiter.try_acquire(peer.ip()) else {
        return onboarding_error(StatusCode::TOO_MANY_REQUESTS, "overloaded");
    };
    let deadline = Instant::now() + ENROLLMENT_TIMEOUT;
    let headers = request.headers();
    if headers.contains_key(header::ORIGIN)
        || headers.contains_key(header::CONTENT_ENCODING)
        || headers.get_all(header::CONTENT_TYPE).iter().count() != 1
        || !headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
    {
        return onboarding_malformed().await;
    }
    if headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > MAX_ENROLLMENT_BYTES as u64)
    {
        return onboarding_error(StatusCode::PAYLOAD_TOO_LARGE, "request_too_large");
    }
    let bytes = match tokio::time::timeout_at(deadline, enrollment_body(request.into_body())).await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err((status, code))) => return onboarding_error(status, code),
        Err(_) => return onboarding_error(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable"),
    };
    let Ok(request) = serde_json::from_slice::<EnrollmentRequest>(&bytes) else {
        return onboarding_malformed().await;
    };
    let invite_id = request.invite_id;
    let (reply, received) = oneshot::channel();
    // The actor owns the slot after enqueue, so a timed-out HTTP handler cannot
    // admit more than eight outstanding store operations.
    match state.handle.controls.try_send(ControlCommand::Enroll {
        request,
        reply,
        deadline,
        _slot: slot,
    }) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            return onboarding_error(StatusCode::TOO_MANY_REQUESTS, "overloaded");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            return onboarding_error(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable");
        }
    }
    match tokio::time::timeout_at(deadline, received).await {
        Ok(Ok(Ok(claims))) => Json(EnrollmentResponse {
            version: ONBOARDING_VERSION,
            server_id: state.handle.server_id,
            invite_id,
            claims,
        })
        .into_response(),
        Ok(Ok(Err(EnrollmentError::Malformed))) => onboarding_malformed().await,
        Ok(Ok(Err(EnrollmentError::InviteUnavailable))) => {
            onboarding_error(StatusCode::UNAUTHORIZED, "invite_unavailable")
        }
        Ok(Ok(Err(EnrollmentError::Unavailable)) | Err(_)) | Err(_) => {
            onboarding_error(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable")
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    service: &'static str,
    protocol_version: u8,
    status: &'static str,
    instance_id: Uuid,
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        service: "agent-session-router",
        protocol_version: PROTOCOL_VERSION,
        status: if state.handle.healthy.load(Ordering::Acquire) {
            "ok"
        } else {
            "unhealthy"
        },
        instance_id: state.instance_id,
    })
}

async fn websocket_upgrade(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    websocket: WebSocketUpgrade,
) -> Response {
    let websocket = websocket
        .max_message_size(crate::protocol::MAX_WEBSOCKET_MESSAGE_BYTES)
        .max_frame_size(crate::protocol::MAX_WEBSOCKET_MESSAGE_BYTES)
        .write_buffer_size(64 * 1024)
        .max_write_buffer_size(WRITER_BYTE_CAPACITY);
    let Some(pre_auth) = state.pre_auth.try_acquire(peer.ip()) else {
        return websocket.on_upgrade(close_overloaded);
    };
    websocket.on_upgrade(move |socket| supervise_socket(state, socket, pre_auth))
}

async fn close_overloaded(mut socket: WebSocket) {
    let _ = tokio::time::timeout(
        WRITE_TIMEOUT,
        socket.send(close_message(1013, "overloaded")),
    )
    .await;
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct SocketClose {
    code: u16,
    reason: &'static str,
}

fn close_message(code: u16, reason: &'static str) -> Message {
    Message::Close(Some(CloseFrame {
        code,
        reason: Utf8Bytes::from_static(reason),
    }))
}

async fn supervise_socket(state: AppState, socket: WebSocket, pre_auth: PreAuthPermit) {
    let connection_id = Uuid::new_v4();
    let generation = state.next_generation.fetch_add(1, Ordering::Relaxed);
    let (writer_tx, mut writer_rx) = mpsc::channel::<OutboundFrame>(WRITER_FRAME_CAPACITY);
    let writer_bytes = Arc::new(Semaphore::new(WRITER_BYTE_CAPACITY));
    let (close_tx, mut close_rx) = watch::channel(None);
    let (open_tx, open_rx) = oneshot::channel();
    let open_sent = state
        .handle
        .send_control(ControlCommand::Open {
            connection_id,
            generation,
            writer: SocketWriter {
                sender: writer_tx,
                bytes: writer_bytes,
                close: close_tx,
            },
            reply: open_tx,
        })
        .await
        .is_ok();
    let opened = open_sent
        && matches!(
            tokio::time::timeout(REGISTRATION_TIMEOUT, open_rx).await,
            Ok(Ok(true))
        );
    if !opened {
        let mut socket = socket;
        let _ = tokio::time::timeout(
            WRITE_TIMEOUT,
            socket.send(close_message(1013, "overloaded")),
        )
        .await;
        if open_sent {
            acknowledge_closed(&state.handle, connection_id, generation).await;
        }
        return;
    }

    let (mut sink, mut stream) = socket.split();
    let registration_deadline = tokio::time::sleep(REGISTRATION_TIMEOUT);
    tokio::pin!(registration_deadline);
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut pre_auth = Some(pre_auth);
    let mut heartbeat_sequence = 0_u64;
    let mut pending_pong: Option<(Vec<u8>, Instant)> = None;
    let mut registered = false;
    let mut final_close = SocketClose {
        code: 1000,
        reason: "closed",
    };
    let mut close_channel_open = true;
    loop {
        let pong_deadline = pending_pong.as_ref().map(|(_, deadline)| *deadline);
        tokio::select! {
            biased;
            changed = close_rx.changed(), if close_channel_open => {
                if changed.is_ok() {
                    if let Some(requested) = *close_rx.borrow_and_update() {
                        final_close = requested;
                        break;
                    }
                } else {
                    close_channel_open = false;
                }
            }
            frame = writer_rx.recv() => {
                let Some(frame) = frame else { break; };
                let write_result =
                    tokio::time::timeout(WRITE_TIMEOUT, sink.send(frame.message)).await;
                if !matches!(write_result, Ok(Ok(()))) {
                    break;
                }
                if frame.registration_ack {
                    registered = true;
                    drop(pre_auth.take());
                } else if !registered && frame.registration_error {
                    final_close = SocketClose {
                        code: 1008,
                        reason: "registration rejected",
                    };
                    break;
                }
            }
            incoming = stream.next() => {
                let Some(incoming) = incoming else { break; };
                let Ok(message) = incoming else {
                    final_close = SocketClose {
                        code: 1009,
                        reason: "message too large",
                    };
                    break;
                };
                match message {
                    Message::Text(text) => {
                        let raw = text.as_str();
                        let parsed = parse_client_message(raw);
                        let byte_len = raw.len();
                        match parsed {
                            Ok(message) => {
                                if !registered && !matches!(message, ClientMessage::Register { .. } | ClientMessage::RegisterDelegate { .. } | ClientMessage::RegisterOperator { .. }) {
                                    let _ = state.handle.send_direct_error(connection_id, generation, None, RouterErrorCode::NotRegistered).await;
                                    final_close = SocketClose {
                                        code: 1008,
                                        reason: "registration required",
                                    };
                                    break;
                                }
                                match state.handle.send_message(connection_id, generation, message, byte_len) {
                                    Ok(()) => {}
                                    Err(IngressSendError::Overloaded) => {
                                        final_close = SocketClose {
                                            code: 1013,
                                            reason: "overloaded",
                                        };
                                        break;
                                    }
                                    Err(IngressSendError::Stopped) => break,
                                }
                            }
                            Err(RouterErrorCode::MessageTooLarge) => {
                                final_close = SocketClose {
                                    code: 1009,
                                    reason: "message too large",
                                };
                                break;
                            }
                            Err(code) => {
                                let terminal = code == RouterErrorCode::ProtocolMismatch;
                                let _ = state.handle.send_direct_error(connection_id, generation, None, code).await;
                                if terminal {
                                    final_close = SocketClose {
                                        code: 1008,
                                        reason: "protocol mismatch",
                                    };
                                    break;
                                }
                            }
                        }
                    }
                    Message::Ping(payload) => {
                        let write_result =
                            tokio::time::timeout(WRITE_TIMEOUT, sink.send(Message::Pong(payload)))
                                .await;
                        if !matches!(write_result, Ok(Ok(()))) {
                            break;
                        }
                    }
                    Message::Pong(payload) => {
                        if pending_pong
                            .as_ref()
                            .is_some_and(|(expected, _)| payload.as_ref() == expected.as_slice())
                        {
                            pending_pong = None;
                        }
                    }
                    Message::Close(_) => break,
                    Message::Binary(_) => {
                        final_close = SocketClose {
                            code: 1003,
                            reason: "text required",
                        };
                        break;
                    }
                }
            }
            () = &mut registration_deadline, if !registered => {
                final_close = SocketClose {
                    code: 1008,
                    reason: "registration timed out",
                };
                break;
            }
            () = async {
                if let Some(deadline) = pong_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                final_close = SocketClose {
                    code: 1001,
                    reason: "heartbeat timed out",
                };
                break;
            }
            _ = heartbeat.tick(), if registered && pending_pong.is_none() => {
                heartbeat_sequence = heartbeat_sequence.wrapping_add(1);
                let payload = heartbeat_sequence.to_be_bytes().to_vec();
                let write_result = tokio::time::timeout(
                    WRITE_TIMEOUT,
                    sink.send(Message::Ping(payload.clone().into())),
                )
                .await;
                if !matches!(write_result, Ok(Ok(()))) {
                    break;
                }
                pending_pong = Some((payload, Instant::now() + HEARTBEAT_TIMEOUT));
            }
        }
    }
    let _ = tokio::time::timeout(
        WRITE_TIMEOUT,
        sink.send(close_message(final_close.code, final_close.reason)),
    )
    .await;
    acknowledge_closed(&state.handle, connection_id, generation).await;
}

async fn acknowledge_closed(handle: &RouterHandle, connection_id: Uuid, generation: i64) {
    let (closed_tx, closed_rx) = oneshot::channel();
    if handle
        .send_control(ControlCommand::Closed {
            connection_id,
            generation,
            reply: closed_tx,
        })
        .await
        .is_ok()
    {
        let _ = tokio::time::timeout(WRITE_TIMEOUT, closed_rx).await;
    }
}

#[derive(Clone)]
pub struct RouterHandle {
    commands: mpsc::Sender<ActorCommand>,
    controls: mpsc::Sender<ControlCommand>,
    command_bytes: Arc<Semaphore>,
    shutdown: Arc<tokio::sync::Notify>,
    healthy: Arc<AtomicBool>,
    server_id: Uuid,
}

impl RouterHandle {
    fn send_message(
        &self,
        connection_id: Uuid,
        generation: i64,
        message: ClientMessage,
        byte_len: usize,
    ) -> Result<(), IngressSendError> {
        let permits = u32::try_from(byte_len.max(1)).map_err(|_| IngressSendError::Overloaded)?;
        let permit = self
            .command_bytes
            .clone()
            .try_acquire_many_owned(permits)
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => IngressSendError::Stopped,
                tokio::sync::TryAcquireError::NoPermits => IngressSendError::Overloaded,
            })?;
        self.commands
            .try_send(ActorCommand {
                connection_id,
                generation,
                message,
                _bytes: permit,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => IngressSendError::Overloaded,
                mpsc::error::TrySendError::Closed(_) => IngressSendError::Stopped,
            })
    }

    async fn send_control(&self, command: ControlCommand) -> Result<(), RouterRuntimeError> {
        self.controls
            .send(command)
            .await
            .map_err(|_| RouterRuntimeError::ActorStopped)
    }

    async fn send_direct_error(
        &self,
        connection_id: Uuid,
        generation: i64,
        request_id: Option<String>,
        code: RouterErrorCode,
    ) -> Result<(), RouterRuntimeError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send_control(ControlCommand::Direct {
            connection_id,
            generation,
            message: Box::new(ServerMessage::Error {
                request_id,
                code,
                workspace: None,
                operation_id: None,
                current_version: None,
            }),
            reply: reply_tx,
        })
        .await?;
        reply_rx.await.map_err(|_| RouterRuntimeError::ActorStopped)
    }

    pub async fn shutdown(&self) -> Result<(), RouterRuntimeError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send_control(ControlCommand::Shutdown { reply: reply_tx })
            .await?;
        reply_rx
            .await
            .map_err(|_| RouterRuntimeError::ActorStopped)??;
        Ok(())
    }

    async fn revoke_credential(&self, id: Uuid) -> Result<bool, RouterRuntimeError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send_control(ControlCommand::RevokeCredential {
            id,
            reply: reply_tx,
        })
        .await?;
        reply_rx
            .await
            .map_err(|_| RouterRuntimeError::ActorStopped)?
    }

    async fn wait_for_shutdown(&self) {
        self.shutdown.notified().await;
    }
}

enum IngressSendError {
    Overloaded,
    Stopped,
}

struct ActorCommand {
    connection_id: Uuid,
    generation: i64,
    message: ClientMessage,
    _bytes: OwnedSemaphorePermit,
}

enum ControlCommand {
    Open {
        connection_id: Uuid,
        generation: i64,
        writer: SocketWriter,
        reply: oneshot::Sender<bool>,
    },
    Closed {
        connection_id: Uuid,
        generation: i64,
        reply: oneshot::Sender<()>,
    },
    Direct {
        connection_id: Uuid,
        generation: i64,
        message: Box<ServerMessage>,
        reply: oneshot::Sender<()>,
    },
    RevokeCredential {
        id: Uuid,
        reply: oneshot::Sender<Result<bool, RouterRuntimeError>>,
    },
    Enroll {
        request: EnrollmentRequest,
        reply: oneshot::Sender<Result<PublicCredentialClaims, EnrollmentError>>,
        deadline: Instant,
        _slot: OwnedSemaphorePermit,
    },
    IntegrationCheckFinished {
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        workspace: WorkspaceName,
        provider: ExternalProvider,
        result: Result<crate::integrations::IntegrationCheck, IntegrationClientError>,
    },
    ExternalGate {
        prepared: PreparedExternalOperation,
        reply: oneshot::Sender<Result<(), RouterErrorCode>>,
    },
    ExternalFinished {
        prepared: PreparedExternalOperation,
        result: ExternalWorkerResult,
    },
    ResolutionFinished {
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        prepared: PreparedExternalResolution,
        result: Result<crate::integrations::ExternalMutation, IntegrationClientError>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<(), RouterRuntimeError>>,
    },
}

#[derive(Clone)]
struct SocketWriter {
    sender: mpsc::Sender<OutboundFrame>,
    bytes: Arc<Semaphore>,
    close: watch::Sender<Option<SocketClose>>,
}

struct OutboundFrame {
    message: Message,
    registration_ack: bool,
    registration_error: bool,
    _bytes: OwnedSemaphorePermit,
}

#[derive(Clone)]
enum ConnectionRole {
    Agent {
        claims: PublicCredentialClaims,
        descriptor: AgentDescriptor,
        delegation_token: Option<String>,
    },
    Delegate {
        owner_id: String,
    },
    Operator {
        claims: PublicCredentialClaims,
        admin: bool,
    },
}

#[derive(Clone, Eq, PartialEq)]
struct CancelingWork {
    workspace: WorkspaceName,
    request_id: String,
    task: Option<TaskFence>,
}

struct ConnectionState {
    generation: i64,
    writer: SocketWriter,
    role: Option<ConnectionRole>,
    workspace: Option<WorkspaceName>,
    subscribed: bool,
    stored_ready: bool,
    canceling: Option<CancelingWork>,
}

struct PendingRequest {
    workspace: WorkspaceName,
    request_id: String,
    requester: Uuid,
    requester_generation: i64,
    requester_id: String,
    recipient: Uuid,
    recipient_generation: i64,
    recipient_id: String,
    deadline: Instant,
    task: Option<TaskDispatch>,
    attempt_id: Option<Uuid>,
}

struct CredentialBucket {
    available_units: u64,
    updated_at: Instant,
}

impl CredentialBucket {
    const UNIT: u64 = 1_000_000_000;

    fn new(now: Instant) -> Self {
        Self {
            available_units: CREDENTIAL_MESSAGE_BURST * Self::UNIT,
            updated_at: now,
        }
    }

    fn allow(&mut self, now: Instant) -> bool {
        let elapsed_units = now
            .duration_since(self.updated_at)
            .as_nanos()
            .saturating_mul(u128::from(CREDENTIAL_MESSAGES_PER_SECOND));
        let capacity = CREDENTIAL_MESSAGE_BURST * Self::UNIT;
        let refilled = u128::from(self.available_units)
            .saturating_add(elapsed_units)
            .min(u128::from(capacity));
        self.available_units = u64::try_from(refilled).unwrap_or(capacity);
        self.updated_at = now;
        if self.available_units < Self::UNIT {
            return false;
        }
        self.available_units -= Self::UNIT;
        true
    }
}

enum ExternalWorkerSuccess {
    Read(crate::integrations::ExternalIssue),
    Mutation(crate::integrations::ExternalMutation),
}

enum ExternalWorkerResult {
    Success(ExternalWorkerSuccess),
    ClientError(IntegrationClientError),
    GateDenied(RouterErrorCode),
}

struct RouterState {
    store: RouterStore,
    connections: HashMap<Uuid, ConnectionState>,
    primaries: HashMap<String, Uuid>,
    delegates: HashMap<String, HashSet<Uuid>>,
    pending: HashMap<(WorkspaceName, String), PendingRequest>,
    credential_buckets: HashMap<Uuid, CredentialBucket>,
    subscribers: HashMap<WorkspaceName, HashSet<Uuid>>,
    integrations: IntegrationRegistry,
    integration_client: Option<IntegrationClient>,
    controls: mpsc::Sender<ControlCommand>,
    integration_requests: usize,
    integration_task_slots: HashSet<(WorkspaceName, i64, ExternalProvider)>,
    shutting_down: bool,
    healthy: Arc<AtomicBool>,
    shutdown: Arc<tokio::sync::Notify>,
}

fn run_actor_thread(
    config: RouterConfig,
    ready: oneshot::Sender<Result<RouterHandle, RouterRuntimeError>>,
) -> Result<(), RouterRuntimeError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(RouterRuntimeError::Io)?;
    runtime.block_on(async move {
        let mut store = RouterStore::open(&config.data_dir)?;
        bootstrap_admin(&mut store, &config.data_dir)?;
        let server_id = store.server_id()?;
        let integrations = IntegrationRegistry::load(&mut store, &config.data_dir);
        let integration_client = if integrations.has_active() {
            IntegrationClient::new(None).ok()
        } else {
            None
        };
        let (command_tx, mut command_rx) = mpsc::channel(ACTOR_COMMAND_CAPACITY);
        let (control_tx, mut control_rx) = mpsc::channel(CONTROL_CAPACITY);
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let healthy = Arc::new(AtomicBool::new(true));
        let handle = RouterHandle {
            commands: command_tx,
            controls: control_tx.clone(),
            command_bytes: Arc::new(Semaphore::new(ACTOR_BYTE_CAPACITY)),
            shutdown: shutdown.clone(),
            healthy: healthy.clone(),
            server_id,
        };
        ready
            .send(Ok(handle))
            .map_err(|_| RouterRuntimeError::ActorStopped)?;
        let mut state = RouterState {
            store,
            connections: HashMap::new(),
            primaries: HashMap::new(),
            delegates: HashMap::new(),
            pending: HashMap::new(),
            credential_buckets: HashMap::new(),
            integrations,
            integration_client,
            controls: control_tx.clone(),
            integration_requests: 0,
            integration_task_slots: HashSet::new(),
            subscribers: HashMap::new(),
            shutting_down: false,
            healthy,
            shutdown,
        };
        let mut terminal_error = None;
        loop {
            let now = Instant::now();
            if state
                .pending
                .values()
                .any(|pending| pending.deadline <= now)
            {
                if let Err(error) = state.expire_deadlines() {
                    state.fail_closed();
                    terminal_error = Some(error);
                    break;
                }
                continue;
            }
            let next_deadline = state.pending.values().map(|pending| pending.deadline).min();
            tokio::select! {
                biased;
                control = control_rx.recv() => {
                    let Some(control) = control else { break; };
                    match state.handle_control(control) {
                        Ok(true) => break,
                        Ok(false) => {}
                        Err(error) => {
                            state.fail_closed();
                            terminal_error = Some(error);
                            break;
                        }
                    }
                }
                () = async {
                    if let Some(deadline) = next_deadline {
                        tokio::time::sleep_until(deadline).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    if let Err(error) = state.expire_deadlines() {
                        state.fail_closed();
                        terminal_error = Some(error);
                        break;
                    }
                }
                command = command_rx.recv() => {
                    let Some(command) = command else { break; };
                    if !state.shutting_down {
                        state.handle_message(command);
                        if state.shutting_down {
                            let fatal = !state.healthy.load(Ordering::Acquire);
                            if let Err(error) = state.shutdown_pending() {
                                terminal_error = Some(error);
                            } else if fatal {
                                terminal_error = Some(RouterRuntimeError::ActorStopped);
                            }
                            state.close_all();
                            state.shutdown.notify_one();
                            break;
                        }
                    }
                }
            }
        }
        let recovery_result = state.store.recover_external_inflight();
        state.close_all();
        let close_result = state.store.close();
        if let Some(error) = terminal_error {
            return Err(error);
        }
        recovery_result?;
        close_result?;
        Ok(())
    })
}

impl RouterState {
    fn fail_closed(&mut self) {
        self.healthy.store(false, Ordering::Release);
        self.shutting_down = true;
        let _ = self.shutdown_pending();
        self.close_all();
        self.shutdown.notify_one();
    }

    fn handle_control(&mut self, control: ControlCommand) -> Result<bool, RouterRuntimeError> {
        match control {
            ControlCommand::Open {
                connection_id,
                generation,
                writer,
                reply,
            } => {
                let accepted = !self.shutting_down;
                if accepted {
                    self.connections.insert(
                        connection_id,
                        ConnectionState {
                            generation,
                            writer,
                            role: None,
                            workspace: None,
                            subscribed: false,
                            stored_ready: false,
                            canceling: None,
                        },
                    );
                }
                let _ = reply.send(accepted);
                Ok(false)
            }
            ControlCommand::Closed {
                connection_id,
                generation,
                reply,
            } => {
                self.disconnect(connection_id, generation)?;
                let _ = reply.send(());
                Ok(false)
            }
            ControlCommand::Direct {
                connection_id,
                generation,
                message,
                reply,
            } => {
                self.send(connection_id, generation, message.as_ref());
                let _ = reply.send(());
                Ok(false)
            }
            ControlCommand::RevokeCredential { id, reply } => match self.revoke_credential(id) {
                Ok(revoked) => {
                    let _ = reply.send(Ok(revoked));
                    Ok(false)
                }
                Err(error) => {
                    let _ = reply.send(Err(RouterRuntimeError::ActorStopped));
                    Err(error)
                }
            },
            ControlCommand::Enroll {
                request,
                reply,
                deadline,
                _slot,
            } => {
                if !reply.is_closed() {
                    let result = if self.shutting_down || Instant::now() >= deadline {
                        Err(EnrollmentError::Unavailable)
                    } else {
                        crate::store::now_millis()
                            .map_err(|_| EnrollmentError::Unavailable)
                            .and_then(|now| self.store.redeem_onboarding(&request, now))
                    };
                    let _ = reply.send(result);
                }
                Ok(false)
            }
            ControlCommand::IntegrationCheckFinished {
                connection_id,
                generation,
                request_id,
                workspace,
                provider,
                result,
            } => {
                self.integration_requests = self.integration_requests.saturating_sub(1);
                match result {
                    Ok(_) => match self.integrations.binding(&workspace, provider) {
                        Ok(active) => {
                            let _ = self.send(
                                connection_id,
                                generation,
                                &ServerMessage::IntegrationChecked {
                                    request_id,
                                    workspace,
                                    integration: active.connection.public(),
                                },
                            );
                        }
                        Err(code) => self.send_error(
                            connection_id,
                            generation,
                            Some(request_id),
                            code,
                            Some(workspace),
                            None,
                            None,
                        ),
                    },
                    Err(error) => self.send_error(
                        connection_id,
                        generation,
                        Some(request_id),
                        integration_router_code(error),
                        Some(workspace),
                        None,
                        None,
                    ),
                }
                Ok(false)
            }
            ControlCommand::ExternalGate { prepared, reply } => {
                let result = if self.shutting_down {
                    Err(RouterErrorCode::ConfigurationRequired)
                } else if Instant::now() >= prepared.deadline {
                    Err(RouterErrorCode::RequestTimeout)
                } else {
                    self.integrations
                        .binding(&prepared.workspace, prepared.summary.provider)
                        .and_then(|active| {
                            validate_external_snapshot(&mut self.store, &active, &prepared, false)
                        })
                        .and_then(|()| mark_external_dispatched(&mut self.store, &prepared))
                };
                let result = result.map(|event| self.fanout(&event));
                let _ = reply.send(result);
                Ok(false)
            }
            ControlCommand::ExternalFinished { prepared, result } => {
                self.release_external_reservation(&prepared);
                let completion = match result {
                    ExternalWorkerResult::Success(ExternalWorkerSuccess::Read(issue)) => self
                        .integrations
                        .binding(&prepared.workspace, prepared.summary.provider)
                        .and_then(|active| {
                            validate_external_snapshot(&mut self.store, &active, &prepared, false)
                        })
                        .and_then(|()| complete_external_read(&mut self.store, &prepared, &issue)),
                    ExternalWorkerResult::Success(ExternalWorkerSuccess::Mutation(mutation)) => {
                        complete_external_mutation(&mut self.store, &prepared, &mutation)
                    }
                    ExternalWorkerResult::ClientError(error) => {
                        complete_external_error(&mut self.store, &prepared, error)
                    }
                    ExternalWorkerResult::GateDenied(code) => terminal_external_error(
                        &mut self.store,
                        &prepared,
                        crate::tasks::ExternalOperationStatus::Failed,
                        code.as_str(),
                    ),
                };
                let completion = match completion {
                    Ok(completion) => completion,
                    Err(code) => terminal_external_error(
                        &mut self.store,
                        &prepared,
                        crate::tasks::ExternalOperationStatus::Failed,
                        code.as_str(),
                    )
                    .map_err(|_| RouterRuntimeError::ActorStopped)?,
                };
                if let Some(event) = completion.task_event {
                    self.fanout(&event);
                }
                self.fanout(&completion.event);
                Ok(false)
            }
            ControlCommand::ResolutionFinished {
                connection_id,
                generation,
                request_id,
                prepared,
                result,
            } => {
                self.release_resolution_reservation(&prepared);
                match result {
                    Err(error) => self.send_error(
                        connection_id,
                        generation,
                        Some(request_id),
                        integration_router_code(error),
                        Some(prepared.workspace),
                        Some(prepared.operation_id),
                        None,
                    ),
                    Ok(mutation) => {
                        let completion = self
                            .integrations
                            .binding(&prepared.workspace, prepared.provider)
                            .and_then(|active| {
                                validate_resolution_snapshot(&self.store, &active, &prepared)
                            })
                            .and_then(|()| {
                                complete_applied_resolution(&mut self.store, &prepared, &mutation)
                            });
                        match completion {
                            Err(code) => self.send_error(
                                connection_id,
                                generation,
                                Some(request_id),
                                code,
                                Some(prepared.workspace),
                                Some(prepared.operation_id),
                                None,
                            ),
                            Ok(completion) => {
                                self.send(
                                    connection_id,
                                    generation,
                                    &ServerMessage::ExternalResolved {
                                        request_id,
                                        workspace: prepared.workspace,
                                        operation: completion.operation,
                                        resolution: completion.resolution,
                                    },
                                );
                                if let Some(event) = completion.task_event {
                                    self.fanout(&event);
                                }
                                self.fanout(&completion.event);
                            }
                        }
                    }
                }
                Ok(false)
            }
            ControlCommand::Shutdown { reply } => {
                self.shutting_down = true;
                self.shutdown.notify_one();
                self.shutdown_pending()?;
                self.close_all();
                let _ = reply.send(Ok(()));
                Ok(true)
            }
        }
    }

    fn release_external_reservation(&mut self, prepared: &PreparedExternalOperation) {
        self.integration_requests = self.integration_requests.saturating_sub(1);
        if let Some(task_id) = prepared.summary.task_id {
            self.integration_task_slots.remove(&(
                prepared.workspace.clone(),
                task_id,
                prepared.summary.provider,
            ));
        }
    }

    fn release_resolution_reservation(&mut self, prepared: &PreparedExternalResolution) {
        self.integration_requests = self.integration_requests.saturating_sub(1);
        if let Some(task_id) = prepared.task_id {
            self.integration_task_slots.remove(&(
                prepared.workspace.clone(),
                task_id,
                prepared.provider,
            ));
        }
    }

    fn handle_message(&mut self, command: ActorCommand) {
        let Some(connection) = self.connections.get(&command.connection_id) else {
            return;
        };
        if connection.generation != command.generation {
            return;
        }
        let request_id = command.message.request_id().map(str::to_owned);
        if let Some(credential_id) = self.connection_credential_id(command.connection_id) {
            let now = Instant::now();
            let allowed = self
                .credential_buckets
                .entry(credential_id)
                .or_insert_with(|| CredentialBucket::new(now))
                .allow(now);
            if !allowed {
                self.send_error(
                    command.connection_id,
                    command.generation,
                    request_id,
                    RouterErrorCode::RateLimited,
                    None,
                    None,
                    None,
                );
                return;
            }
        }
        let result = self.dispatch(command.connection_id, command.generation, command.message);
        if let Err(error) = result {
            let fatal_storage_error = error.code == RouterErrorCode::StorageError;
            self.send_error(
                command.connection_id,
                command.generation,
                request_id,
                error.code,
                error.workspace,
                error.operation_id,
                error.current_version,
            );
            if fatal_storage_error {
                self.healthy.store(false, Ordering::Release);
                self.shutting_down = true;
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn dispatch(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        message: ClientMessage,
    ) -> Result<(), ActorError> {
        match message {
            ClientMessage::Register {
                agent,
                token,
                delegation_token,
                ..
            } => self.register_agent(connection_id, generation, agent, &token, delegation_token),
            ClientMessage::RegisterDelegate {
                owner_id,
                delegation_token,
                ..
            } => self.register_delegate(connection_id, generation, &owner_id, &delegation_token),
            ClientMessage::RegisterOperator { token, .. } => {
                self.register_operator(connection_id, generation, &token)
            }
            ClientMessage::Ping { request_id } => {
                self.require_registered(connection_id)?;
                self.send(
                    connection_id,
                    generation,
                    &ServerMessage::Pong { request_id },
                );
                Ok(())
            }
            ClientMessage::Readiness { ready } => self.set_readiness(connection_id, ready),
            ClientMessage::List { request_id } | ClientMessage::WorkspaceMembers { request_id } => {
                let workspace = self.current_workspace(connection_id)?;
                let agents = self.agents_in_workspace(&workspace);
                self.send(
                    connection_id,
                    generation,
                    &ServerMessage::Agents {
                        request_id,
                        workspace,
                        agents,
                    },
                );
                Ok(())
            }
            ClientMessage::Send {
                request_id,
                to,
                content,
                timeout_ms,
            } => self.start_request(
                connection_id,
                generation,
                request_id,
                to,
                content,
                timeout_ms,
                None,
            ),
            ClientMessage::Reply {
                request_id,
                ok,
                content,
                error,
            } => self.finish_reply(connection_id, generation, &request_id, ok, content, error),
            ClientMessage::WorkIdle {
                request_id,
                workspace,
                work_request_id,
                task,
                ready,
            } => self.work_idle(
                connection_id,
                generation,
                request_id,
                workspace,
                work_request_id,
                task,
                ready,
            ),
            ClientMessage::TaskExecutionStopped {
                request_id,
                workspace,
                task_id,
                attempt_id,
                ended_session_id,
                evidence,
                reason,
            } => self.task_execution_stopped(
                connection_id,
                generation,
                request_id,
                workspace,
                task_id,
                attempt_id,
                ended_session_id,
                evidence,
                reason,
            ),
            ClientMessage::CredentialIssue {
                request_id,
                role,
                subject,
                agent_side,
                agent_client,
                workspaces,
            } => {
                self.require_admin(connection_id)?;
                if role == CredentialRole::Operator && subject == "admin" {
                    return Err(ActorError::code(RouterErrorCode::PermissionDenied));
                }
                for workspace in &workspaces {
                    if !self.store.workspace_exists(workspace)? {
                        return Err(ActorError::code(RouterErrorCode::WorkspaceNotFound));
                    }
                }
                let credential =
                    CredentialFile::generate(role, subject, agent_side, agent_client, workspaces)
                        .map_err(|_| ActorError::code(RouterErrorCode::InvalidMessage))?;
                self.store.insert_credential(&credential)?;
                self.send(
                    connection_id,
                    generation,
                    &ServerMessage::CredentialIssued {
                        request_id,
                        credential,
                    },
                );
                Ok(())
            }
            ClientMessage::CredentialList {
                request_id,
                after,
                limit,
            } => {
                self.require_admin(connection_id)?;
                let limit = limit.unwrap_or(crate::protocol::DEFAULT_PAGE_LIMIT);
                if !(1..=crate::protocol::MAX_PAGE_LIMIT).contains(&limit) {
                    return Err(ActorError::code(RouterErrorCode::InvalidMessage));
                }
                let (credentials, next_cursor, has_more) =
                    self.store.list_credentials(after, limit)?;
                let credentials = credentials
                    .into_iter()
                    .map(|credential| CredentialSummary {
                        claims: credential.claims,
                        created_at: credential.created_at,
                        revoked_at: credential.revoked_at,
                    })
                    .collect();
                self.send(
                    connection_id,
                    generation,
                    &ServerMessage::Credentials {
                        request_id,
                        credentials,
                        next_cursor,
                        has_more,
                    },
                );
                Ok(())
            }
            ClientMessage::CredentialRevoke { request_id, id } => {
                self.require_admin(connection_id)?;
                let revoked = self
                    .revoke_credential(id)
                    .map_err(|_| ActorError::code(RouterErrorCode::StorageError))?;
                if !revoked {
                    return Err(ActorError::code(RouterErrorCode::RequestNotFound));
                }
                self.send(
                    connection_id,
                    generation,
                    &ServerMessage::CredentialRevoked { request_id, id },
                );
                Ok(())
            }
            ClientMessage::OnboardingInviteIssue {
                request_id,
                workspace,
                create_workspace,
                provider,
            } => {
                self.require_admin(connection_id)?;
                let invite = self.store.issue_onboarding_invite(
                    &workspace,
                    create_workspace,
                    provider,
                    crate::store::now_millis()?,
                )?;
                self.send(
                    connection_id,
                    generation,
                    &ServerMessage::OnboardingInviteIssued {
                        request_id,
                        server_id: invite.server_id,
                        invite_id: invite.invite_id,
                        invite_token: invite.invite_token,
                        expires_at: invite.expires_at,
                        workspace: invite.workspace,
                        provider: invite.provider,
                    },
                );
                Ok(())
            }
            ClientMessage::OnboardingInviteRevoke {
                request_id,
                invite_id,
            } => {
                self.require_admin(connection_id)?;
                self.store
                    .revoke_onboarding_invite(invite_id, crate::store::now_millis()?)?;
                self.send(
                    connection_id,
                    generation,
                    &ServerMessage::OnboardingInviteRevoked {
                        request_id,
                        invite_id,
                    },
                );
                Ok(())
            }
            ClientMessage::WorkspaceCreate { request_id, name } => {
                self.require_admin(connection_id)?;
                let (created_at, created) = self.store.create_workspace(&name)?;
                if !created {
                    return Err(ActorError::code(RouterErrorCode::WorkspaceConflict));
                }
                self.send(
                    connection_id,
                    generation,
                    &ServerMessage::WorkspaceCreated {
                        request_id,
                        workspace: WorkspaceSummary {
                            name,
                            created_at,
                            connected_agents: 0,
                        },
                    },
                );
                Ok(())
            }
            ClientMessage::WorkspaceList {
                request_id,
                after,
                limit,
            } => self.workspace_list(
                connection_id,
                generation,
                request_id,
                after.as_deref(),
                limit,
            ),
            ClientMessage::WorkspaceJoin { request_id, name } => {
                self.workspace_join(connection_id, generation, request_id, name)
            }
            ClientMessage::WorkspaceLeave { request_id } => {
                self.workspace_leave(connection_id, generation, request_id)
            }
            ClientMessage::WorkspacePost {
                request_id,
                content,
            } => self.workspace_post(connection_id, generation, request_id, &content),
            ClientMessage::WorkspaceHistory {
                request_id,
                after,
                limit,
            } => self.workspace_history(connection_id, generation, request_id, after, limit),
            ClientMessage::WorkspaceSubscribe { request_id, after } => {
                self.workspace_subscribe(connection_id, generation, request_id, after)
            }
            ClientMessage::WorkspaceUnsubscribe { request_id } => {
                self.workspace_unsubscribe(connection_id, generation, request_id)
            }
            ClientMessage::TaskList {
                request_id,
                workspace,
                states,
                assigned_agent_id,
                after,
                limit,
            } => self.task_list(
                connection_id,
                generation,
                request_id,
                workspace,
                states,
                assigned_agent_id.as_deref(),
                after,
                limit,
            ),
            ClientMessage::TaskGet {
                request_id,
                workspace,
                task_id,
            } => self.task_get(connection_id, generation, request_id, workspace, task_id),
            ClientMessage::TaskCreate {
                request_id,
                workspace,
                operation_id,
                title,
                description,
            } => self.task_mutate(
                connection_id,
                generation,
                request_id,
                workspace,
                &TaskCommand::Create {
                    operation_id,
                    title,
                    description,
                },
            ),
            ClientMessage::TaskEdit {
                request_id,
                workspace,
                operation_id,
                task_id,
                expected_version,
                title,
                description,
            } => self.task_mutate(
                connection_id,
                generation,
                request_id,
                workspace,
                &TaskCommand::Edit {
                    operation_id,
                    task_id,
                    expected_version,
                    title,
                    description,
                },
            ),
            ClientMessage::TaskAssign {
                request_id,
                workspace,
                operation_id,
                task_id,
                expected_version,
                agent_id,
            } => self.task_mutate(
                connection_id,
                generation,
                request_id,
                workspace,
                &TaskCommand::Assign {
                    operation_id,
                    task_id,
                    expected_version,
                    agent_id,
                },
            ),
            ClientMessage::TaskNote {
                request_id,
                workspace,
                operation_id,
                task_id,
                text,
            } => self.task_mutate(
                connection_id,
                generation,
                request_id,
                workspace,
                &TaskCommand::Note {
                    operation_id,
                    task_id,
                    text,
                },
            ),
            ClientMessage::TaskBegin {
                request_id,
                workspace,
                operation_id,
                task_id,
                work_request_id,
                expected_version,
                last_checkpoint_id,
                resume_note,
            } => self.task_mutate(
                connection_id,
                generation,
                request_id,
                workspace,
                &TaskCommand::Begin {
                    operation_id,
                    task_id,
                    work_request_id,
                    expected_version,
                    last_checkpoint_id,
                    resume_note,
                },
            ),
            ClientMessage::TaskCheckpoint {
                request_id,
                workspace,
                operation_id,
                task_id,
                attempt_id,
                expected_version,
                checkpoint,
            } => self.task_mutate(
                connection_id,
                generation,
                request_id,
                workspace,
                &TaskCommand::Checkpoint {
                    operation_id,
                    task_id,
                    attempt_id,
                    expected_version,
                    checkpoint,
                },
            ),
            ClientMessage::TaskPause {
                request_id,
                workspace,
                operation_id,
                task_id,
                attempt_id,
                expected_version,
                checkpoint,
                reason,
            } => self.task_mutate(
                connection_id,
                generation,
                request_id,
                workspace,
                &TaskCommand::Pause {
                    operation_id,
                    task_id,
                    attempt_id,
                    expected_version,
                    checkpoint,
                    blocked: matches!(reason, crate::protocol::TaskPauseKind::Blocked),
                },
            ),
            ClientMessage::TaskComplete {
                request_id,
                workspace,
                operation_id,
                task_id,
                attempt_id,
                expected_version,
                result,
            } => self.task_mutate(
                connection_id,
                generation,
                request_id,
                workspace,
                &TaskCommand::Complete {
                    operation_id,
                    task_id,
                    attempt_id,
                    expected_version,
                    result,
                },
            ),
            ClientMessage::TaskCancel {
                request_id,
                workspace,
                operation_id,
                task_id,
                expected_version,
                note,
            } => self.task_mutate(
                connection_id,
                generation,
                request_id,
                workspace,
                &TaskCommand::Cancel {
                    operation_id,
                    task_id,
                    expected_version,
                    note,
                },
            ),
            ClientMessage::TaskReopen {
                request_id,
                workspace,
                operation_id,
                task_id,
                expected_version,
                note,
            } => self.task_mutate(
                connection_id,
                generation,
                request_id,
                workspace,
                &TaskCommand::Reopen {
                    operation_id,
                    task_id,
                    expected_version,
                    note,
                },
            ),
            ClientMessage::TaskInterrupt {
                request_id,
                workspace,
                operation_id,
                task_id,
                expected_version,
                note,
            } => self.task_mutate(
                connection_id,
                generation,
                request_id,
                workspace,
                &TaskCommand::Interrupt {
                    operation_id,
                    task_id,
                    expected_version,
                    note,
                },
            ),
            ClientMessage::TaskConfirmStopped {
                request_id,
                workspace,
                operation_id,
                task_id,
                attempt_id,
                expected_version,
                note,
            } => self.task_mutate(
                connection_id,
                generation,
                request_id,
                workspace,
                &TaskCommand::ConfirmStopped {
                    operation_id,
                    task_id,
                    attempt_id,
                    expected_version,
                    note,
                },
            ),
            ClientMessage::TaskRequest {
                request_id,
                workspace,
                task_id,
                expected_version,
                message,
                timeout_ms,
            } => self.task_request(
                connection_id,
                generation,
                request_id,
                workspace,
                task_id,
                expected_version,
                message,
                timeout_ms,
            ),
            ClientMessage::RouterShutdown { request_id } => {
                self.require_admin(connection_id)?;
                self.send(
                    connection_id,
                    generation,
                    &ServerMessage::RouterStopping { request_id },
                );
                self.shutting_down = true;
                Ok(())
            }
            ClientMessage::TaskImport {
                request_id,
                workspace,
                provider,
                external_id,
                operation_id,
            } => self.external_start(
                connection_id,
                generation,
                request_id,
                workspace,
                &ExternalCommand::Import {
                    provider,
                    external_id,
                    operation_id,
                },
            ),
            ClientMessage::TaskLink {
                request_id,
                workspace,
                operation_id,
                task_id,
                expected_version,
                provider,
                external_id,
                replace,
            } => self.external_start(
                connection_id,
                generation,
                request_id,
                workspace,
                &ExternalCommand::Link {
                    provider,
                    external_id,
                    task_id,
                    expected_version,
                    operation_id,
                    replace,
                },
            ),
            ClientMessage::TaskPublish {
                request_id,
                workspace,
                operation_id,
                task_id,
                expected_version,
                provider,
                kind,
                report_id,
            } => self.external_start(
                connection_id,
                generation,
                request_id,
                workspace,
                &ExternalCommand::Publish {
                    provider,
                    task_id,
                    expected_version,
                    operation_id,
                    kind,
                    report_id,
                },
            ),
            ClientMessage::TaskExternalStatus {
                request_id,
                workspace,
                operation_id,
            } => self.external_status(
                connection_id,
                generation,
                request_id,
                workspace,
                operation_id,
            ),
            ClientMessage::TaskExternalResolve {
                request_id,
                workspace,
                operation_id,
                resolution_id,
                outcome,
                external_id,
                note,
            } => self.external_resolve(
                connection_id,
                generation,
                request_id,
                workspace,
                operation_id,
                resolution_id,
                outcome,
                external_id.as_deref(),
                &note,
            ),
            ClientMessage::IntegrationList {
                request_id,
                workspace,
            } => self.integration_list(connection_id, generation, request_id, workspace),
            ClientMessage::IntegrationCheck {
                request_id,
                workspace,
                provider,
            } => self.integration_check(connection_id, generation, request_id, workspace, provider),
            ClientMessage::IntegrationReload { request_id } => {
                self.integration_reload(connection_id, generation, request_id)
            }
            ClientMessage::TaskHistory {
                request_id,
                workspace,
                task_id,
                after,
                limit,
            } => self.task_history(
                connection_id,
                generation,
                request_id,
                workspace,
                task_id,
                after,
                limit,
            ),
        }
    }

    fn register_agent(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        registration: AgentRegistration,
        token: &str,
        delegation_token: Option<String>,
    ) -> Result<(), ActorError> {
        self.require_unregistered(connection_id)?;
        let claims = self.authenticate(token)?;
        if self.primaries.contains_key(&registration.agent_id) {
            return Err(ActorError::code(RouterErrorCode::AgentConflict));
        }
        if claims.role != CredentialRole::Agent
            || claims.subject != registration.agent_id
            || claims.agent_side != Some(registration.side)
            || claims.agent_client != Some(registration.client)
        {
            return Err(ActorError::code(RouterErrorCode::Unauthorized));
        }
        let descriptor = AgentDescriptor {
            agent_id: registration.agent_id.clone(),
            side: registration.side,
            client: registration.client,
            activity: registration.activity,
            status: AgentStatus::Idle,
            delivery_mode: registration.delivery_mode,
            ready: false,
            session_id: Uuid::new_v4(),
        };
        let connection = self
            .connections
            .get_mut(&connection_id)
            .ok_or_else(|| ActorError::code(RouterErrorCode::NotRegistered))?;
        connection.role = Some(ConnectionRole::Agent {
            claims,
            descriptor: descriptor.clone(),
            delegation_token,
        });
        self.primaries
            .insert(registration.agent_id.clone(), connection_id);
        self.send(
            connection_id,
            generation,
            &ServerMessage::Registered {
                protocol_version: PROTOCOL_VERSION,
                agent: descriptor,
                role: RegistrationRole::Agent,
                workspace: None,
                cursor: 0,
            },
        );
        self.send_execution_snapshot(connection_id, generation, registration.agent_id.as_str())?;
        Ok(())
    }

    fn register_delegate(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        owner_id: &str,
        token: &str,
    ) -> Result<(), ActorError> {
        self.require_unregistered(connection_id)?;
        let owner_connection_id = *self
            .primaries
            .get(owner_id)
            .ok_or_else(|| ActorError::code(RouterErrorCode::Unauthorized))?;
        let owner = self
            .connections
            .get(&owner_connection_id)
            .ok_or_else(|| ActorError::code(RouterErrorCode::Unauthorized))?;
        let (descriptor, workspace) = match owner.role.as_ref() {
            Some(ConnectionRole::Agent {
                descriptor,
                delegation_token: Some(expected),
                ..
            }) if expected == token => (descriptor.clone(), owner.workspace.clone()),
            _ => return Err(ActorError::code(RouterErrorCode::Unauthorized)),
        };
        let cursor = workspace
            .as_ref()
            .and_then(|name| self.store.latest_seq(name).ok().flatten())
            .unwrap_or(0);
        let connection = self
            .connections
            .get_mut(&connection_id)
            .ok_or_else(|| ActorError::code(RouterErrorCode::NotRegistered))?;
        connection.role = Some(ConnectionRole::Delegate {
            owner_id: owner_id.to_owned(),
        });
        connection.workspace.clone_from(&workspace);
        self.delegates
            .entry(owner_id.to_owned())
            .or_default()
            .insert(connection_id);
        self.send(
            connection_id,
            generation,
            &ServerMessage::Registered {
                protocol_version: PROTOCOL_VERSION,
                agent: descriptor,
                role: RegistrationRole::Delegate,
                workspace,
                cursor,
            },
        );
        Ok(())
    }

    fn register_operator(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        token: &str,
    ) -> Result<(), ActorError> {
        self.require_unregistered(connection_id)?;
        let claims = self.authenticate(token)?;
        if claims.role != CredentialRole::Operator {
            return Err(ActorError::code(RouterErrorCode::Unauthorized));
        }
        let admin = claims.subject == "admin" && claims.workspaces.is_empty();
        let subject = claims.subject.clone();
        let connection = self
            .connections
            .get_mut(&connection_id)
            .ok_or_else(|| ActorError::code(RouterErrorCode::NotRegistered))?;
        connection.role = Some(ConnectionRole::Operator { claims, admin });
        self.send(
            connection_id,
            generation,
            &ServerMessage::RegisteredOperator {
                protocol_version: PROTOCOL_VERSION,
                subject,
                admin,
                workspace: None,
                cursor: 0,
            },
        );
        Ok(())
    }

    fn authenticate(&self, token: &str) -> Result<PublicCredentialClaims, ActorError> {
        let credential = self
            .store
            .credential_by_hash(&hash_token(token))?
            .ok_or_else(|| ActorError::code(RouterErrorCode::Unauthorized))?;
        if credential.revoked_at.is_some() {
            return Err(ActorError::code(RouterErrorCode::Unauthorized));
        }
        Ok(credential.claims)
    }

    fn set_readiness(&mut self, connection_id: Uuid, ready: bool) -> Result<(), ActorError> {
        let Some(connection) = self.connections.get_mut(&connection_id) else {
            return Err(ActorError::code(RouterErrorCode::NotRegistered));
        };
        let canceling = connection.canceling.is_some();
        match connection.role.as_mut() {
            Some(ConnectionRole::Agent { descriptor, .. })
                if descriptor.delivery_mode == DeliveryMode::Pull =>
            {
                let blocked =
                    agent_has_execution_barrier(&self.store, &descriptor.agent_id).unwrap_or(true);
                connection.stored_ready = ready;
                descriptor.ready = ready
                    && !canceling
                    && !blocked
                    && descriptor.status == AgentStatus::Idle
                    && connection.workspace.is_some();
                Ok(())
            }
            Some(ConnectionRole::Agent { .. }) => {
                Err(ActorError::code(RouterErrorCode::InvalidMessage))
            }
            _ => Err(ActorError::code(RouterErrorCode::PermissionDenied)),
        }
    }

    fn workspace_list(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        after: Option<&str>,
        limit: Option<u16>,
    ) -> Result<(), ActorError> {
        let (claims, admin) = self.claims(connection_id)?;
        let limit = limit.unwrap_or(crate::protocol::DEFAULT_PAGE_LIMIT);
        if !(1..=crate::protocol::MAX_PAGE_LIMIT).contains(&limit) {
            return Err(ActorError::code(RouterErrorCode::InvalidMessage));
        }
        if let Some(after) = after {
            WorkspaceName::parse(after)
                .map_err(|_| ActorError::code(RouterErrorCode::InvalidMessage))?;
        }
        let allowed = if admin {
            None
        } else {
            Some(claims.workspaces.as_slice())
        };
        let (rows, _, source_has_more) = self.store.list_workspace_rows(allowed, after, limit)?;
        let mut workspaces = rows
            .into_iter()
            .map(|(name, created_at)| {
                let connected_agents = u32::try_from(self.agents_in_workspace(&name).len())
                    .map_err(|_| ActorError::code(RouterErrorCode::StorageError))?;
                Ok(WorkspaceSummary {
                    name,
                    created_at,
                    connected_agents,
                })
            })
            .collect::<Result<Vec<_>, ActorError>>()?;
        let (next_cursor, has_more) =
            bound_workspace_list(&request_id, &mut workspaces, after, source_has_more)?;
        self.send(
            connection_id,
            generation,
            &ServerMessage::Workspaces {
                request_id,
                workspaces,
                next_cursor,
                has_more,
            },
        );
        Ok(())
    }

    fn workspace_join(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        name: WorkspaceName,
    ) -> Result<(), ActorError> {
        let effective_connection = self
            .effective_primary(connection_id)
            .unwrap_or(connection_id);
        self.authorize_workspace(effective_connection, &name)?;
        if !self.store.workspace_exists(&name)? {
            return Err(ActorError::code(RouterErrorCode::WorkspaceNotFound));
        }
        let current = self
            .connections
            .get(&effective_connection)
            .and_then(|connection| connection.workspace.clone());
        if current.as_ref() == Some(&name) {
            let cursor = self.store.latest_seq(&name)?.unwrap_or(0);
            self.send(
                connection_id,
                generation,
                &ServerMessage::WorkspaceJoined {
                    request_id,
                    workspace: name,
                    cursor,
                },
            );
            return Ok(());
        }
        if current.is_some() {
            return Err(ActorError::code(RouterErrorCode::WorkspaceAlreadyJoined));
        }
        let is_operator = matches!(
            self.connections
                .get(&effective_connection)
                .and_then(|value| value.role.as_ref()),
            Some(ConnectionRole::Operator { .. })
        );
        let actor = self.actor_id(connection_id)?;
        let event = if is_operator {
            None
        } else {
            Some(self.store.append_event(&EventInsert {
                workspace: &name,
                kind: WorkspaceEventKind::MemberJoined,
                actor_id: &actor,
                request_id: None,
                target_id: None,
                task_id: None,
                content: None,
                ok: None,
                error: None,
            })?)
        };
        let cursor = match event.as_ref() {
            Some(event) => event.seq,
            None => self.store.latest_seq(&name)?.unwrap_or(0),
        };
        if let Some(connection) = self.connections.get_mut(&effective_connection) {
            connection.workspace = Some(name.clone());
            if let Some(ConnectionRole::Agent { descriptor, .. }) = connection.role.as_mut() {
                let blocked =
                    agent_has_execution_barrier(&self.store, &descriptor.agent_id).unwrap_or(true);
                connection.stored_ready = descriptor.delivery_mode == DeliveryMode::Push;
                descriptor.ready = connection.stored_ready && !blocked;
            }
        }
        self.sync_delegates(effective_connection, Some(&name));
        self.send(
            connection_id,
            generation,
            &ServerMessage::WorkspaceJoined {
                request_id,
                workspace: name.clone(),
                cursor,
            },
        );
        self.notify_membership(effective_connection, Some(name.clone()), cursor);
        if let Some(event) = event {
            self.fanout(&event);
        }
        Ok(())
    }

    fn workspace_leave(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
    ) -> Result<(), ActorError> {
        let effective_connection = self
            .effective_primary(connection_id)
            .unwrap_or(connection_id);
        let Some(workspace) = self
            .connections
            .get(&effective_connection)
            .and_then(|connection| connection.workspace.clone())
        else {
            self.send(
                connection_id,
                generation,
                &ServerMessage::WorkspaceLeft {
                    request_id,
                    workspace: None,
                },
            );
            return Ok(());
        };
        if self
            .connections
            .get(&effective_connection)
            .is_some_and(|connection| connection.canceling.is_some())
        {
            return Err(ActorError::code(RouterErrorCode::WorkspaceBusy));
        }
        let connection_family = self.connection_family(effective_connection);
        if self.pending.values().any(|pending| {
            pending.workspace == workspace
                && (connection_family.contains(&pending.requester)
                    || connection_family.contains(&pending.recipient))
        }) {
            return Err(ActorError::code(RouterErrorCode::WorkspaceBusy));
        }
        let agent_id = self
            .connections
            .get(&effective_connection)
            .and_then(|connection| match connection.role.as_ref() {
                Some(ConnectionRole::Agent { descriptor, .. }) => {
                    Some(descriptor.agent_id.as_str())
                }
                _ => None,
            });
        if let Some(agent_id) = agent_id {
            let (running, unconfirmed) = agent_execution_barriers(&self.store, agent_id)?;
            if running {
                return Err(ActorError::code(RouterErrorCode::WorkspaceBusy));
            }
            if unconfirmed {
                return Err(ActorError::code(RouterErrorCode::LeaveUnconfirmed));
            }
        }
        let is_operator = matches!(
            self.connections
                .get(&effective_connection)
                .and_then(|value| value.role.as_ref()),
            Some(ConnectionRole::Operator { .. })
        );
        let actor = self.actor_id(connection_id)?;
        let event = if is_operator {
            None
        } else {
            Some(self.store.append_event(&EventInsert {
                workspace: &workspace,
                kind: WorkspaceEventKind::MemberLeft,
                actor_id: &actor,
                request_id: None,
                target_id: None,
                task_id: None,
                content: None,
                ok: None,
                error: None,
            })?)
        };
        if let Some(connection) = self.connections.get_mut(&effective_connection) {
            connection.workspace = None;
            connection.stored_ready = false;
            if let Some(ConnectionRole::Agent { descriptor, .. }) = connection.role.as_mut() {
                descriptor.ready = false;
            }
        }
        self.sync_delegates(effective_connection, None);
        self.remove_subscriptions_for_owner(effective_connection);
        self.send(
            connection_id,
            generation,
            &ServerMessage::WorkspaceLeft {
                request_id,
                workspace: Some(workspace.clone()),
            },
        );
        self.notify_membership(effective_connection, None, 0);
        if let Some(event) = event {
            self.fanout(&event);
        }
        Ok(())
    }

    fn workspace_post(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        content: &str,
    ) -> Result<(), ActorError> {
        let workspace = self.current_workspace(connection_id)?;
        let actor = self.actor_id(connection_id)?;
        let appended =
            self.store
                .append_chat_idempotent(&workspace, &actor, &request_id, content)?;
        self.send(
            connection_id,
            generation,
            &ServerMessage::WorkspacePosted {
                request_id,
                workspace: workspace.clone(),
                seq: appended.event.seq,
            },
        );
        if appended.inserted {
            self.fanout(&appended.event);
        }
        Ok(())
    }

    fn workspace_history(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        after: Option<i64>,
        limit: Option<u16>,
    ) -> Result<(), ActorError> {
        let workspace = self.current_workspace(connection_id)?;
        let recent = after.is_none();
        let (after, limit) = validate_page(after, limit).map_err(ActorError::code)?;
        let latest = self.store.latest_seq(&workspace)?.unwrap_or(0);
        if !recent && after > latest {
            return Err(ActorError::code(RouterErrorCode::InvalidMessage));
        }
        let (mut events, _, source_has_more) = if recent {
            self.store.recent_history(&workspace, limit)?
        } else {
            self.store.history(&workspace, after, limit)?
        };
        let (next_cursor, has_more) = bound_history_page(
            "workspace_history",
            &request_id,
            &workspace,
            &mut events,
            after,
            source_has_more,
        )?;
        self.send(
            connection_id,
            generation,
            &ServerMessage::WorkspaceHistory {
                request_id,
                page: HistoryPage {
                    workspace,
                    events,
                    next_cursor,
                    has_more,
                },
            },
        );
        Ok(())
    }

    fn workspace_subscribe(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        after: i64,
    ) -> Result<(), ActorError> {
        let workspace = self.current_workspace(connection_id)?;
        if matches!(
            self.connections
                .get(&connection_id)
                .and_then(|value| value.role.as_ref()),
            Some(ConnectionRole::Delegate { .. })
        ) {
            return Err(ActorError::code(RouterErrorCode::PermissionDenied));
        }
        let latest = self.store.latest_seq(&workspace)?.unwrap_or(0);
        if after > latest {
            return Err(ActorError::code(RouterErrorCode::InvalidMessage));
        }
        let (mut events, _, source_has_more) =
            self.store
                .history(&workspace, after, crate::protocol::MAX_PAGE_LIMIT)?;
        let (next_cursor, has_more) = bound_history_page(
            "workspace_subscription",
            &request_id,
            &workspace,
            &mut events,
            after,
            source_has_more,
        )?;
        let live = !has_more;
        if live {
            self.subscribers
                .entry(workspace.clone())
                .or_default()
                .insert(connection_id);
            if let Some(connection) = self.connections.get_mut(&connection_id) {
                connection.subscribed = true;
            }
        }
        self.send(
            connection_id,
            generation,
            &ServerMessage::WorkspaceSubscription {
                request_id,
                workspace,
                events,
                next_cursor,
                live,
            },
        );
        Ok(())
    }

    fn workspace_unsubscribe(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
    ) -> Result<(), ActorError> {
        let workspace = self.current_workspace(connection_id)?;
        if let Some(subscribers) = self.subscribers.get_mut(&workspace) {
            subscribers.remove(&connection_id);
        }
        if let Some(connection) = self.connections.get_mut(&connection_id) {
            connection.subscribed = false;
        }
        self.send(
            connection_id,
            generation,
            &ServerMessage::WorkspaceUnsubscribed {
                request_id,
                workspace,
            },
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn start_request(
        &mut self,
        requester: Uuid,
        requester_generation: i64,
        request_id: String,
        recipient_id: String,
        content: String,
        timeout_ms: Option<u64>,
        task: Option<TaskDispatch>,
    ) -> Result<(), ActorError> {
        let workspace = self.current_workspace(requester)?;
        let effective_requester = self.effective_primary(requester).unwrap_or(requester);
        if self
            .connections
            .get(&effective_requester)
            .is_some_and(|connection| connection.canceling.is_some())
        {
            return Err(ActorError::code(RouterErrorCode::SessionBusy));
        }
        if let Some(agent_id) = self
            .connections
            .get(&effective_requester)
            .and_then(|connection| match connection.role.as_ref() {
                Some(ConnectionRole::Agent { descriptor, .. }) => {
                    Some(descriptor.agent_id.as_str())
                }
                _ => None,
            })
        {
            let (_, stop_pending) = agent_execution_barriers(&self.store, agent_id)?;
            if stop_pending {
                return Err(ActorError::code(RouterErrorCode::SessionBusy));
            }
        }
        if self.pending.len() >= PENDING_REQUEST_LIMIT {
            return Err(ActorError::code(RouterErrorCode::RateLimited));
        }
        if self
            .pending
            .contains_key(&(workspace.clone(), request_id.clone()))
        {
            return Err(ActorError::code(RouterErrorCode::RequestConflict));
        }
        let recipient = *self
            .primaries
            .get(&recipient_id)
            .ok_or_else(|| ActorError::code(RouterErrorCode::TargetOffline))?;
        let recipient_connection = self
            .connections
            .get(&recipient)
            .ok_or_else(|| ActorError::code(RouterErrorCode::TargetOffline))?;
        if recipient_connection.workspace.as_ref() != Some(&workspace) {
            return Err(ActorError::code(RouterErrorCode::TargetOffline));
        }
        let Some(ConnectionRole::Agent { descriptor, .. }) = recipient_connection.role.as_ref()
        else {
            return Err(ActorError::code(RouterErrorCode::TargetOffline));
        };
        if recipient_connection.canceling.is_some() || descriptor.status == AgentStatus::Busy {
            return Err(ActorError::code(RouterErrorCode::SessionBusy));
        }
        if agent_has_execution_barrier(&self.store, &descriptor.agent_id)? {
            return Err(ActorError::code(RouterErrorCode::SessionBusy));
        }
        if !descriptor.ready {
            return Err(ActorError::code(RouterErrorCode::TargetNotReady));
        }
        let recipient_generation = recipient_connection.generation;
        let requester_id = self.actor_id(requester)?;
        let now = Instant::now();
        let requested_timeout_ms = normalize_timeout_ms(timeout_ms);
        let requested_deadline = now + Duration::from_millis(requested_timeout_ms);
        let deadline = self
            .pending
            .values()
            .filter(|pending| pending.recipient == effective_requester)
            .map(|pending| pending.deadline)
            .min()
            .map_or(requested_deadline, |parent| parent.min(requested_deadline));
        let timeout_ms = u64::try_from(deadline.saturating_duration_since(now).as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let event = self.store.append_request(&EventInsert {
            workspace: &workspace,
            kind: WorkspaceEventKind::Request,
            actor_id: &requester_id,
            request_id: Some(&request_id),
            target_id: Some(&recipient_id),
            task_id: task.as_ref().map(|dispatch| dispatch.id),
            content: Some(&content),
            ok: None,
            error: None,
        })?;
        let pending = PendingRequest {
            workspace: workspace.clone(),
            request_id: request_id.clone(),
            requester,
            requester_generation,
            requester_id,
            recipient,
            recipient_generation,
            recipient_id: recipient_id.clone(),
            deadline,
            task: task.clone(),
            attempt_id: None,
        };
        self.pending
            .insert((workspace.clone(), request_id.clone()), pending);
        self.set_busy(recipient, true);
        if !self.send(
            recipient,
            recipient_generation,
            &ServerMessage::Deliver {
                workspace: workspace.clone(),
                request_id: request_id.clone(),
                from: self.actor_id(requester)?,
                content,
                timeout_ms,
                task,
            },
        ) {
            self.finish_pending_error(
                &workspace,
                &request_id,
                RouterErrorCode::TargetDisconnected,
            )?;
            return Err(ActorError::code(RouterErrorCode::TargetDisconnected));
        }
        self.send(
            requester,
            requester_generation,
            &ServerMessage::Accepted {
                request_id,
                workspace,
                to: recipient_id,
            },
        );
        self.fanout(&event);
        Ok(())
    }

    fn finish_reply(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: &str,
        ok: bool,
        content: Option<String>,
        error: Option<RouterErrorCode>,
    ) -> Result<(), ActorError> {
        let workspace = self.current_workspace(connection_id)?;
        let key = (workspace.clone(), request_id.to_owned());
        let Some(current) = self.pending.get(&key) else {
            return Err(ActorError::code(RouterErrorCode::RequestNotFound));
        };
        if current.recipient != connection_id || current.recipient_generation != generation {
            return Err(ActorError::code(RouterErrorCode::PermissionDenied));
        }
        let pending = self.pending.remove(&key).expect("pending checked above");
        let task_event = if let Some(attempt_id) = pending.attempt_id {
            interrupt_attempt(
                &mut self.store,
                attempt_id,
                if ok {
                    PauseReason::ReplyWithoutRelease
                } else {
                    PauseReason::HostError
                },
            )?
        } else {
            None
        };
        let attempt_change = if let Some(attempt_id) = pending.attempt_id {
            execution_attempt(&self.store, attempt_id)?
        } else {
            None
        };
        let (content, error) = if ok {
            (Some(content.unwrap_or_default()), None)
        } else {
            (None, Some(error.unwrap_or(RouterErrorCode::ProviderError)))
        };
        let task_id = pending.task.as_ref().map(|task| task.id);
        let event = self.store.append_event(&EventInsert {
            workspace: &workspace,
            kind: WorkspaceEventKind::Result,
            actor_id: &pending.recipient_id,
            request_id: Some(&pending.request_id),
            target_id: Some(&pending.requester_id),
            task_id,
            content: content.as_deref(),
            ok: Some(ok),
            error,
        })?;
        self.set_busy(connection_id, false);
        self.send(
            pending.requester,
            pending.requester_generation,
            &ServerMessage::Result {
                workspace: workspace.clone(),
                request_id: pending.request_id,
                from: pending.recipient_id.clone(),
                ok,
                content,
                error,
                task_id,
            },
        );
        if let Some((attempt_workspace, attempt)) = attempt_change {
            if attempt_workspace != workspace || Some(attempt.task_id) != task_id {
                return Err(StoreError::InvalidData.into());
            }
            self.notify_task_attempt_changed(
                workspace,
                attempt.task_id,
                Some(attempt),
                pending.attempt_id,
                &pending.recipient_id,
            )?;
        }
        if let Some(task_event) = task_event {
            self.fanout(&task_event);
        }
        self.fanout(&event);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn work_idle(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        workspace: WorkspaceName,
        work_request_id: String,
        task: Option<TaskFence>,
        ready: bool,
    ) -> Result<(), ActorError> {
        self.require_workspace_match(connection_id, &workspace)?;
        let expected = CancelingWork {
            workspace: workspace.clone(),
            request_id: work_request_id.clone(),
            task,
        };
        let owned_by_other = self.connections.iter().any(|(candidate, connection)| {
            *candidate != connection_id && connection.canceling.as_ref() == Some(&expected)
        });
        let agent_id = match self.connections.get(&connection_id) {
            Some(ConnectionState {
                generation: current_generation,
                role: Some(ConnectionRole::Agent { descriptor, .. }),
                canceling: Some(canceling),
                ..
            }) if *current_generation == generation && canceling == &expected => {
                descriptor.agent_id.clone()
            }
            Some(ConnectionState {
                role: Some(ConnectionRole::Agent { .. }),
                ..
            }) if owned_by_other => {
                return Err(ActorError::code(RouterErrorCode::PermissionDenied));
            }
            Some(ConnectionState {
                role: Some(ConnectionRole::Agent { .. }),
                ..
            }) => return Err(ActorError::code(RouterErrorCode::RequestNotFound)),
            _ => return Err(ActorError::code(RouterErrorCode::PermissionDenied)),
        };
        let blocked = agent_has_execution_barrier(&self.store, &agent_id)?;
        if let Some(connection) = self.connections.get_mut(&connection_id) {
            connection.canceling = None;
            connection.stored_ready = ready;
            if let Some(ConnectionRole::Agent { descriptor, .. }) = connection.role.as_mut() {
                descriptor.status = AgentStatus::Idle;
                descriptor.ready = ready && !blocked && connection.workspace.is_some();
            }
        }
        self.send(
            connection_id,
            generation,
            &ServerMessage::WorkIdleAck {
                request_id,
                workspace,
                work_request_id,
            },
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn task_execution_stopped(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        workspace: WorkspaceName,
        task_id: i64,
        attempt_id: Uuid,
        ended_session_id: Uuid,
        _evidence: TaskExecutionEvidence,
        reason: PauseReason,
    ) -> Result<(), ActorError> {
        self.require_workspace_match(connection_id, &workspace)?;
        let (agent_id, credential_id) = match self
            .connections
            .get(&connection_id)
            .and_then(|connection| connection.role.as_ref())
        {
            Some(ConnectionRole::Agent {
                claims, descriptor, ..
            }) => (descriptor.agent_id.clone(), claims.id),
            _ => return Err(ActorError::code(RouterErrorCode::PermissionDenied)),
        };
        let event = record_execution_stopped(
            &mut self.store,
            &workspace,
            task_id,
            attempt_id,
            ended_session_id,
            &agent_id,
            credential_id,
            reason,
        )
        .map_err(|error| ActorError {
            code: error.code,
            workspace: Some(workspace.clone()),
            operation_id: None,
            current_version: error.current_version,
        })?;
        if !self
            .pending
            .values()
            .any(|pending| pending.recipient == connection_id)
        {
            self.set_busy(connection_id, false);
        }
        let Some((attempt_workspace, attempt)) = execution_attempt(&self.store, attempt_id)? else {
            return Err(StoreError::InvalidData.into());
        };
        if attempt_workspace != workspace || attempt.task_id != task_id {
            return Err(StoreError::InvalidData.into());
        }
        self.send(
            connection_id,
            generation,
            &ServerMessage::TaskExecutionStoppedAck {
                request_id,
                workspace: workspace.clone(),
                task_id,
                attempt_id,
            },
        );
        self.notify_task_attempt_changed(
            workspace,
            task_id,
            Some(attempt),
            Some(attempt_id),
            &agent_id,
        )?;
        if let Some(event) = event {
            self.fanout(&event);
        }
        Ok(())
    }

    fn task_request(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        workspace: WorkspaceName,
        task_id: i64,
        expected_version: i64,
        message: Option<String>,
        timeout_ms: Option<u64>,
    ) -> Result<(), ActorError> {
        self.require_workspace_match(connection_id, &workspace)?;
        let task = get_task(&self.store, &workspace, task_id)?
            .ok_or_else(|| ActorError::code(RouterErrorCode::TaskNotFound))?;
        if task.summary.version != expected_version {
            return Err(ActorError {
                code: RouterErrorCode::TaskConflict,
                workspace: Some(workspace),
                operation_id: None,
                current_version: Some(task.summary.version),
            });
        }
        if task.summary.state.is_terminal() {
            return Err(ActorError::code(RouterErrorCode::TaskInvalidTransition));
        }
        if task.summary.current_attempt_id.is_some() {
            return Err(ActorError::code(RouterErrorCode::TaskActive));
        }
        if task.summary.stop_evidence == Some(crate::tasks::StopEvidence::Unknown) {
            return Err(ActorError::code(RouterErrorCode::TaskStopUnconfirmed));
        }
        let agent_id = task
            .summary
            .assigned_agent_id
            .clone()
            .ok_or_else(|| ActorError::code(RouterErrorCode::TaskNotAssigned))?;
        if agent_has_execution_barrier(&self.store, &agent_id)? {
            return Err(ActorError::code(RouterErrorCode::TaskStopUnconfirmed));
        }
        if self.pending.values().any(|pending| {
            pending.workspace == workspace
                && pending
                    .task
                    .as_ref()
                    .is_some_and(|dispatch| dispatch.id == task_id)
        }) {
            return Err(ActorError::code(RouterErrorCode::TaskActive));
        }
        self.start_request(
            connection_id,
            generation,
            request_id,
            agent_id,
            message.unwrap_or_else(|| {
                "Perform the assigned task and record checkpoints before releasing it.".to_owned()
            }),
            timeout_ms,
            Some(TaskDispatch {
                id: task_id,
                expected_version,
            }),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn task_list(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        workspace: WorkspaceName,
        states: Option<Vec<TaskState>>,
        assigned_agent_id: Option<&str>,
        after: Option<i64>,
        limit: Option<u16>,
    ) -> Result<(), ActorError> {
        self.require_workspace_match(connection_id, &workspace)?;
        let states = states.unwrap_or_else(|| {
            vec![
                TaskState::Todo,
                TaskState::InProgress,
                TaskState::Blocked,
                TaskState::Paused,
            ]
        });
        if states.is_empty() || states.len() > 6 {
            return Err(ActorError::code(RouterErrorCode::InvalidMessage));
        }
        let unique = states.iter().copied().collect::<HashSet<_>>();
        if unique.len() != states.len() {
            return Err(ActorError::code(RouterErrorCode::InvalidMessage));
        }
        let (after, limit) = validate_page(after, limit).map_err(ActorError::code)?;
        let (mut tasks, _, source_has_more) = list_tasks(
            &self.store,
            &workspace,
            &states,
            assigned_agent_id,
            after,
            limit,
        )?;
        let (next_cursor, has_more) =
            bound_task_list(&request_id, &workspace, &mut tasks, after, source_has_more)?;
        self.send(
            connection_id,
            generation,
            &ServerMessage::Tasks {
                request_id,
                workspace,
                tasks,
                next_cursor,
                has_more,
            },
        );
        Ok(())
    }

    fn task_get(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        workspace: WorkspaceName,
        task_id: i64,
    ) -> Result<(), ActorError> {
        self.require_workspace_match(connection_id, &workspace)?;
        let task = get_task(&self.store, &workspace, task_id)?
            .ok_or_else(|| ActorError::code(RouterErrorCode::TaskNotFound))?;
        self.send(
            connection_id,
            generation,
            &ServerMessage::Task {
                request_id,
                workspace,
                task,
            },
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn task_history(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        workspace: WorkspaceName,
        task_id: i64,
        after: Option<i64>,
        limit: Option<u16>,
    ) -> Result<(), ActorError> {
        self.require_workspace_match(connection_id, &workspace)?;
        if get_task(&self.store, &workspace, task_id)?.is_none() {
            return Err(ActorError::code(RouterErrorCode::TaskNotFound));
        }
        let (after, limit) = validate_page(after, limit).map_err(ActorError::code)?;
        let latest = self.store.latest_seq(&workspace)?.unwrap_or(0);
        if after > latest {
            return Err(ActorError::code(RouterErrorCode::InvalidMessage));
        }
        let (events, _, source_has_more) =
            self.store.task_history(&workspace, task_id, after, limit)?;
        let mut events = events
            .into_iter()
            .map(|stored| {
                let crate::store::StoredTaskHistoryEvent {
                    event: stored,
                    attempt,
                    report,
                } = stored;
                let event = stored
                    .content
                    .as_deref()
                    .ok_or_else(|| ActorError::code(RouterErrorCode::StorageError))
                    .and_then(|content| {
                        serde_json::from_str::<TaskEvent>(content)
                            .map_err(|_| ActorError::code(RouterErrorCode::StorageError))
                    })?;
                if stored.task_id != Some(task_id)
                    || event.task.id != task_id
                    || event.task.workspace != workspace.as_str()
                    || event.attempt_id != attempt.as_ref().map(|attempt| attempt.id)
                    || event.report_id != report.as_ref().map(|report| report.id)
                    || attempt
                        .as_ref()
                        .is_some_and(|attempt| attempt.task_id != task_id)
                    || report
                        .as_ref()
                        .is_some_and(|report| report.task_id != task_id)
                {
                    return Err(ActorError::code(RouterErrorCode::StorageError));
                }
                Ok(TaskHistoryEvent {
                    seq: stored.seq,
                    actor_id: stored.actor_id,
                    created_at: stored.created_at,
                    event,
                    attempt,
                    report,
                })
            })
            .collect::<Result<Vec<_>, ActorError>>()?;
        let (next_cursor, has_more) = bound_task_history(
            &request_id,
            &workspace,
            task_id,
            &mut events,
            after,
            source_has_more,
        )?;
        self.send(
            connection_id,
            generation,
            &ServerMessage::TaskHistory {
                request_id,
                page: TaskHistoryPage {
                    workspace,
                    task_id,
                    events,
                    next_cursor,
                    has_more,
                },
            },
        );
        Ok(())
    }

    fn integration_list(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        workspace: WorkspaceName,
    ) -> Result<(), ActorError> {
        self.require_workspace_match(connection_id, &workspace)?;
        let integrations = self
            .integrations
            .list(&workspace)
            .map_err(|code| ActorError {
                code,
                workspace: Some(workspace.clone()),
                operation_id: None,
                current_version: None,
            })?;
        self.send(
            connection_id,
            generation,
            &ServerMessage::Integrations {
                request_id,
                workspace,
                integrations,
            },
        );
        Ok(())
    }

    fn integration_check(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        workspace: WorkspaceName,
        provider: ExternalProvider,
    ) -> Result<(), ActorError> {
        self.require_operator(connection_id)?;
        self.authorize_workspace(connection_id, &workspace)?;
        if self.integration_requests >= INTEGRATION_REQUEST_LIMIT {
            return Err(ActorError::code(RouterErrorCode::IntegrationBusy));
        }
        let active = self
            .integrations
            .binding(&workspace, provider)
            .map_err(|code| ActorError {
                code,
                workspace: Some(workspace.clone()),
                operation_id: None,
                current_version: None,
            })?;
        let client = self
            .integration_client
            .clone()
            .ok_or_else(|| ActorError::code(RouterErrorCode::IntegrationConfigurationInvalid))?;
        self.integration_requests += 1;
        let controls = self.controls.clone();
        tokio::spawn(async move {
            let result = client.check(&active.connection).await;
            let _ = controls
                .send(ControlCommand::IntegrationCheckFinished {
                    connection_id,
                    generation,
                    request_id,
                    workspace,
                    provider,
                    result,
                })
                .await;
        });
        Ok(())
    }

    fn integration_reload(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
    ) -> Result<(), ActorError> {
        self.require_admin(connection_id)?;
        let (integrations, events) =
            self.integrations
                .reload(&mut self.store)
                .map_err(|code| ActorError {
                    code,
                    workspace: None,
                    operation_id: None,
                    current_version: None,
                })?;
        self.integration_client = if self.integrations.has_active() {
            Some(
                IntegrationClient::new(None)
                    .map_err(|error| ActorError::code(integration_router_code(error)))?,
            )
        } else {
            None
        };
        self.send(
            connection_id,
            generation,
            &ServerMessage::IntegrationsReloaded {
                request_id,
                integrations,
            },
        );
        for event in events {
            self.fanout(&event);
        }
        Ok(())
    }

    fn external_status(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        workspace: WorkspaceName,
        operation_id: Uuid,
    ) -> Result<(), ActorError> {
        self.require_workspace_match(connection_id, &workspace)?;
        let (operation, resolution) = load_external_status(&self.store, &workspace, operation_id)
            .map_err(|code| ActorError {
                code,
                workspace: Some(workspace.clone()),
                operation_id: Some(operation_id),
                current_version: None,
            })?
            .ok_or_else(|| ActorError::code(RouterErrorCode::IntegrationError))?;
        self.send(
            connection_id,
            generation,
            &ServerMessage::ExternalStatus {
                request_id,
                workspace,
                operation,
                resolution,
            },
        );
        Ok(())
    }

    fn external_start(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        workspace: WorkspaceName,
        command: &ExternalCommand,
    ) -> Result<(), ActorError> {
        self.require_workspace_match(connection_id, &workspace)?;
        let caller = self.external_caller_context(connection_id, &workspace)?;
        if let Some(operation) =
            replay_external_operation(&self.store, &caller, command).map_err(|code| ActorError {
                code,
                workspace: Some(workspace.clone()),
                operation_id: Some(command.operation_id()),
                current_version: None,
            })?
        {
            self.send(
                connection_id,
                generation,
                &ServerMessage::ExternalOperation {
                    request_id,
                    workspace,
                    operation,
                },
            );
            return Ok(());
        }
        let active = self
            .integrations
            .binding(&workspace, command.provider())
            .map_err(|code| ActorError {
                code,
                workspace: Some(workspace.clone()),
                operation_id: Some(command.operation_id()),
                current_version: None,
            })?;
        if self.integration_requests >= INTEGRATION_REQUEST_LIMIT {
            return Err(ActorError::code(RouterErrorCode::IntegrationBusy));
        }
        let slot = command
            .task_id()
            .map(|task_id| (workspace.clone(), task_id, command.provider()));
        if slot
            .as_ref()
            .is_some_and(|key| self.integration_task_slots.contains(key))
        {
            return Err(ActorError::code(RouterErrorCode::IntegrationBusy));
        }
        let client = self
            .integration_client
            .clone()
            .ok_or_else(|| ActorError::code(RouterErrorCode::IntegrationConfigurationInvalid))?;
        self.integration_requests += 1;
        if let Some(slot) = slot {
            self.integration_task_slots.insert(slot);
        }
        let prepared = match prepare_external_operation(&mut self.store, &caller, &active, command)
        {
            Ok(prepared) => prepared,
            Err(code) => {
                self.integration_requests = self.integration_requests.saturating_sub(1);
                if let Some(task_id) = command.task_id() {
                    self.integration_task_slots.remove(&(
                        workspace.clone(),
                        task_id,
                        command.provider(),
                    ));
                }
                return Err(ActorError {
                    code,
                    workspace: Some(workspace),
                    operation_id: Some(command.operation_id()),
                    current_version: None,
                });
            }
        };
        self.send(
            connection_id,
            generation,
            &ServerMessage::ExternalOperation {
                request_id,
                workspace: workspace.clone(),
                operation: prepared.summary.clone(),
            },
        );
        if let Some(event) = prepared.event.as_ref() {
            self.fanout(event);
        }
        if prepared.payload.is_none() {
            self.release_external_reservation(&prepared);
            return Ok(());
        }
        let controls = self.controls.clone();
        tokio::spawn(run_external_worker(client, active, prepared, controls));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn external_resolve(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        workspace: WorkspaceName,
        operation_id: Uuid,
        resolution_id: Uuid,
        outcome: ExternalResolutionOutcome,
        external_id: Option<&str>,
        note: &str,
    ) -> Result<(), ActorError> {
        self.require_operator(connection_id)?;
        self.authorize_workspace(connection_id, &workspace)?;
        let (operation, _) = load_external_status(&self.store, &workspace, operation_id)
            .map_err(|code| ActorError {
                code,
                workspace: Some(workspace.clone()),
                operation_id: Some(operation_id),
                current_version: None,
            })?
            .ok_or_else(|| ActorError::code(RouterErrorCode::IntegrationError))?;
        let active = self
            .integrations
            .binding(&workspace, operation.provider)
            .map_err(|code| ActorError {
                code,
                workspace: Some(workspace.clone()),
                operation_id: Some(operation_id),
                current_version: None,
            })?;
        let caller = self.external_caller_context(connection_id, &workspace)?;
        let prepared = prepare_external_resolution(
            &mut self.store,
            &caller,
            &active,
            operation_id,
            resolution_id,
            outcome,
            external_id,
            note,
        )
        .map_err(|code| ActorError {
            code,
            workspace: Some(workspace.clone()),
            operation_id: Some(operation_id),
            current_version: None,
        })?;
        match prepared {
            ResolutionPreparation::Replay {
                operation,
                resolution,
            } => {
                self.send(
                    connection_id,
                    generation,
                    &ServerMessage::ExternalResolved {
                        request_id,
                        workspace,
                        operation,
                        resolution,
                    },
                );
            }
            ResolutionPreparation::Completed(completion) => {
                self.send(
                    connection_id,
                    generation,
                    &ServerMessage::ExternalResolved {
                        request_id,
                        workspace,
                        operation: completion.operation,
                        resolution: completion.resolution,
                    },
                );
                if let Some(event) = completion.task_event {
                    self.fanout(&event);
                }
                self.fanout(&completion.event);
            }
            ResolutionPreparation::Verify(prepared) => {
                if self.integration_requests >= INTEGRATION_REQUEST_LIMIT {
                    return Err(ActorError::code(RouterErrorCode::IntegrationBusy));
                }
                let slot = prepared
                    .task_id
                    .map(|task_id| (workspace.clone(), task_id, prepared.provider));
                if slot
                    .as_ref()
                    .is_some_and(|key| self.integration_task_slots.contains(key))
                {
                    return Err(ActorError::code(RouterErrorCode::IntegrationBusy));
                }
                let client = self.integration_client.clone().ok_or_else(|| {
                    ActorError::code(RouterErrorCode::IntegrationConfigurationInvalid)
                })?;
                self.integration_requests += 1;
                if let Some(slot) = slot {
                    self.integration_task_slots.insert(slot);
                }
                let controls = self.controls.clone();
                tokio::spawn(run_resolution_worker(
                    client,
                    active,
                    prepared,
                    connection_id,
                    generation,
                    request_id,
                    controls,
                ));
            }
        }
        Ok(())
    }

    fn task_mutate(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: String,
        workspace: WorkspaceName,
        command: &TaskCommand,
    ) -> Result<(), ActorError> {
        self.require_workspace_match(connection_id, &workspace)?;
        let attempt_changed = matches!(
            command,
            TaskCommand::Begin { .. }
                | TaskCommand::Pause { .. }
                | TaskCommand::Complete { .. }
                | TaskCommand::Interrupt { .. }
                | TaskCommand::ConfirmStopped { .. }
        );
        if let Some(task_id) = task_command_id(command)
            && !matches!(
                command,
                TaskCommand::Begin { .. }
                    | TaskCommand::Note { .. }
                    | TaskCommand::Checkpoint { .. }
                    | TaskCommand::Pause { .. }
                    | TaskCommand::Complete { .. }
                    | TaskCommand::Interrupt { .. }
            )
            && self.pending.values().any(|pending| {
                pending.workspace == workspace
                    && pending
                        .task
                        .as_ref()
                        .is_some_and(|dispatch| dispatch.id == task_id)
            })
        {
            return Err(ActorError::code(RouterErrorCode::TaskActive));
        }
        let interrupted_request = if let TaskCommand::Interrupt { task_id, .. } = command {
            self.pending
                .iter()
                .find(|((pending_workspace, _), pending)| {
                    pending_workspace == &workspace
                        && pending
                            .task
                            .as_ref()
                            .is_some_and(|dispatch| dispatch.id == *task_id)
                })
                .map(|((pending_workspace, pending_request), _)| {
                    (pending_workspace.clone(), pending_request.clone())
                })
        } else {
            None
        };
        let interrupted_pending = interrupted_request.is_some();
        let stopped_agent = if let TaskCommand::ConfirmStopped { attempt_id, .. } = command {
            attempt_agent_id(&self.store, *attempt_id)?
        } else {
            None
        };
        let caller = self.caller_context(connection_id, &workspace, command)?;
        let result = apply_task(&mut self.store, &caller, command).map_err(|error| ActorError {
            code: error.code,
            workspace: Some(workspace.clone()),
            operation_id: error.operation_id,
            current_version: error.current_version,
        })?;
        if let TaskCommand::Begin {
            work_request_id, ..
        } = command
            && let Some(pending) = self
                .pending
                .get_mut(&(workspace.clone(), work_request_id.clone()))
        {
            pending.attempt_id = result.mutation.task.summary.current_attempt_id;
        }
        if let Some(agent_id) = stopped_agent
            && let Some(connection_id) = self.primaries.get(&agent_id).copied()
        {
            self.set_busy(connection_id, false);
        }
        let attempt_notification = attempt_changed
            .then(|| {
                result
                    .mutation
                    .task
                    .current_attempt
                    .as_ref()
                    .or(result.mutation.task.last_attempt.as_ref())
                    .cloned()
            })
            .flatten();
        let closed_attempt_id = result.closed_attempt_id;
        self.send(
            connection_id,
            generation,
            &ServerMessage::TaskMutated {
                request_id,
                workspace: workspace.clone(),
                result: result.mutation,
            },
        );
        if let Some((pending_workspace, pending_request)) = interrupted_request {
            self.finish_pending_error(
                &pending_workspace,
                &pending_request,
                RouterErrorCode::TaskInterrupted,
            )?;
        }
        if !interrupted_pending && let Some(attempt) = attempt_notification {
            let agent_id = attempt.agent_id.clone();
            self.notify_task_attempt_changed(
                workspace,
                attempt.task_id,
                Some(attempt),
                closed_attempt_id,
                &agent_id,
            )?;
        }
        self.fanout(&result.event);
        Ok(())
    }

    fn caller_context(
        &self,
        connection_id: Uuid,
        workspace: &WorkspaceName,
        command: &TaskCommand,
    ) -> Result<CallerContext, ActorError> {
        let effective = self
            .effective_primary(connection_id)
            .unwrap_or(connection_id);
        let connection = self
            .connections
            .get(&effective)
            .ok_or_else(|| ActorError::code(RouterErrorCode::NotRegistered))?;
        if matches!(command, TaskCommand::Begin { .. }) {
            if connection.canceling.is_some() {
                return Err(ActorError::code(RouterErrorCode::SessionBusy));
            }
            if let Some(ConnectionRole::Agent { descriptor, .. }) = connection.role.as_ref() {
                let (_, stop_pending) =
                    agent_execution_barriers(&self.store, &descriptor.agent_id)?;
                if stop_pending {
                    return Err(ActorError::code(RouterErrorCode::SessionBusy));
                }
            }
        }
        let (role, agent_id, credential_id, session_id) = match connection.role.as_ref() {
            Some(ConnectionRole::Agent {
                claims, descriptor, ..
            }) => (
                if effective == connection_id {
                    CallerRole::Agent
                } else {
                    CallerRole::Delegate
                },
                Some(descriptor.agent_id.clone()),
                claims.id,
                Some(descriptor.session_id),
            ),
            Some(ConnectionRole::Operator { claims, admin }) => (
                if *admin {
                    CallerRole::Admin
                } else {
                    CallerRole::Operator
                },
                None,
                claims.id,
                None,
            ),
            _ => return Err(ActorError::code(RouterErrorCode::NotRegistered)),
        };
        let reservation = if let TaskCommand::Begin {
            task_id,
            work_request_id,
            ..
        } = command
        {
            self.pending
                .get(&(workspace.clone(), work_request_id.clone()))
                .filter(|pending| {
                    pending.recipient == effective
                        && pending
                            .task
                            .as_ref()
                            .is_some_and(|task| task.id == *task_id)
                })
                .map(|_| ReservationFence {
                    work_request_id: work_request_id.clone(),
                    task_id: *task_id,
                })
        } else {
            None
        };
        Ok(CallerContext {
            actor_id: self.actor_id(connection_id)?,
            role,
            workspace: workspace.clone(),
            agent_id,
            credential_id,
            session_id,
            connection_generation: connection.generation,
            reservation,
        })
    }

    fn external_caller_context(
        &self,
        connection_id: Uuid,
        workspace: &WorkspaceName,
    ) -> Result<CallerContext, ActorError> {
        let effective = self
            .effective_primary(connection_id)
            .unwrap_or(connection_id);
        let connection = self
            .connections
            .get(&effective)
            .ok_or_else(|| ActorError::code(RouterErrorCode::NotRegistered))?;
        let (role, agent_id, credential_id, session_id) = match connection.role.as_ref() {
            Some(ConnectionRole::Agent {
                claims, descriptor, ..
            }) => (
                if effective == connection_id {
                    CallerRole::Agent
                } else {
                    CallerRole::Delegate
                },
                Some(descriptor.agent_id.clone()),
                claims.id,
                Some(descriptor.session_id),
            ),
            Some(ConnectionRole::Operator { claims, admin }) => (
                if *admin {
                    CallerRole::Admin
                } else {
                    CallerRole::Operator
                },
                None,
                claims.id,
                None,
            ),
            _ => return Err(ActorError::code(RouterErrorCode::NotRegistered)),
        };
        Ok(CallerContext {
            actor_id: self.actor_id(connection_id)?,
            role,
            workspace: workspace.clone(),
            agent_id,
            credential_id,
            session_id,
            connection_generation: connection.generation,
            reservation: None,
        })
    }

    fn expire_deadlines(&mut self) -> Result<(), RouterRuntimeError> {
        let now = Instant::now();
        let expired = self
            .pending
            .values()
            .filter(|pending| pending.deadline <= now)
            .map(|pending| (pending.workspace.clone(), pending.request_id.clone()))
            .collect::<Vec<_>>();
        for (workspace, request_id) in expired {
            self.finish_pending_error(&workspace, &request_id, RouterErrorCode::RequestTimeout)?;
        }
        Ok(())
    }

    fn finish_pending_error(
        &mut self,
        workspace: &WorkspaceName,
        request_id: &str,
        code: RouterErrorCode,
    ) -> Result<(), StoreError> {
        let Some(pending) = self
            .pending
            .remove(&(workspace.clone(), request_id.to_owned()))
        else {
            return Ok(());
        };
        let task_event = if let Some(attempt_id) = pending.attempt_id {
            interrupt_attempt(&mut self.store, attempt_id, terminal_pause_reason(code))?
        } else {
            None
        };
        let attempt_change = if let Some(attempt_id) = pending.attempt_id {
            execution_attempt(&self.store, attempt_id)?
        } else {
            None
        };
        let event = self.store.append_event(&EventInsert {
            workspace,
            kind: WorkspaceEventKind::Result,
            actor_id: "system:router",
            request_id: Some(request_id),
            target_id: Some(&pending.requester_id),
            task_id: pending.task.as_ref().map(|task| task.id),
            content: None,
            ok: Some(false),
            error: Some(code),
        })?;
        let task = pending
            .task
            .as_ref()
            .zip(pending.attempt_id)
            .map(|(task, attempt_id)| TaskFence {
                task_id: task.id,
                attempt_id,
            });
        let canceling = CancelingWork {
            workspace: workspace.clone(),
            request_id: request_id.to_owned(),
            task: task.clone(),
        };
        let cancel_recipient = self
            .connections
            .get_mut(&pending.recipient)
            .filter(|connection| connection.generation == pending.recipient_generation)
            .and_then(|connection| {
                let ConnectionRole::Agent { descriptor, .. } = connection.role.as_mut()? else {
                    return None;
                };
                descriptor.status = AgentStatus::Busy;
                descriptor.ready = false;
                connection.stored_ready = false;
                connection.canceling = Some(canceling);
                Some(())
            })
            .is_some();
        if cancel_recipient {
            self.send(
                pending.recipient,
                pending.recipient_generation,
                &ServerMessage::CancelWork {
                    workspace: workspace.clone(),
                    request_id: request_id.to_owned(),
                    reason: code,
                    task,
                },
            );
        }
        if let Some((attempt_workspace, attempt)) = attempt_change {
            if attempt_workspace != *workspace {
                return Err(StoreError::InvalidData);
            }
            self.notify_task_attempt_changed(
                workspace.clone(),
                attempt.task_id,
                Some(attempt),
                pending.attempt_id,
                &pending.recipient_id,
            )?;
        }
        self.send(
            pending.requester,
            pending.requester_generation,
            &ServerMessage::Result {
                workspace: workspace.clone(),
                request_id: request_id.to_owned(),
                from: "system:router".to_owned(),
                ok: false,
                content: None,
                error: Some(code),
                task_id: pending.task.map(|task| task.id),
            },
        );
        if let Some(task_event) = task_event {
            self.fanout(&task_event);
        }
        self.fanout(&event);
        Ok(())
    }

    fn revoke_credential(&mut self, credential_id: Uuid) -> Result<bool, RouterRuntimeError> {
        if !self.store.revoke_credential(credential_id)? {
            return Ok(false);
        }
        let matching = self
            .connections
            .iter()
            .filter_map(|(connection_id, connection)| {
                let matches = match connection.role.as_ref() {
                    Some(
                        ConnectionRole::Agent { claims, .. }
                        | ConnectionRole::Operator { claims, .. },
                    ) => claims.id == credential_id,
                    _ => false,
                };
                matches.then_some((*connection_id, connection.generation))
            })
            .collect::<Vec<_>>();
        for (connection_id, generation) in matching {
            for affected in self.connection_family(connection_id) {
                if let Some(affected_generation) = self
                    .connections
                    .get(&affected)
                    .map(|connection| connection.generation)
                {
                    self.send_error(
                        affected,
                        affected_generation,
                        None,
                        RouterErrorCode::Unauthorized,
                        None,
                        None,
                        None,
                    );
                }
            }
            self.disconnect_with_reason(connection_id, generation, PauseReason::CredentialRevoked)?;
        }
        Ok(true)
    }

    fn disconnect(
        &mut self,
        connection_id: Uuid,
        generation: i64,
    ) -> Result<(), RouterRuntimeError> {
        self.disconnect_with_reason(connection_id, generation, PauseReason::TransportLost)
    }

    fn disconnect_with_reason(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        reason: PauseReason,
    ) -> Result<(), RouterRuntimeError> {
        let Some(connection) = self.connections.get(&connection_id) else {
            return Ok(());
        };
        if connection.generation != generation {
            return Ok(());
        }
        let role = connection.role.clone();
        let workspace = connection.workspace.clone();
        let actor = self.actor_id(connection_id).ok();
        let family = self.connection_family(connection_id);
        let credential_id = self.connection_credential_id(connection_id);
        let interrupted = match role.as_ref() {
            Some(ConnectionRole::Agent {
                claims, descriptor, ..
            }) => interrupt_session_attempts(
                &mut self.store,
                &descriptor.agent_id,
                claims.id,
                descriptor.session_id,
                generation,
                reason,
            )?,
            _ => Vec::new(),
        };
        for affected in &family {
            self.connections.remove(affected);
            for subscribers in self.subscribers.values_mut() {
                subscribers.remove(affected);
            }
        }
        if let Some(credential_id) = credential_id {
            let still_connected =
                self.connections.keys().copied().any(|candidate| {
                    self.connection_credential_id(candidate) == Some(credential_id)
                });
            if !still_connected {
                self.credential_buckets.remove(&credential_id);
            }
        }
        for event in interrupted {
            self.fanout(&event);
        }
        match role {
            Some(ConnectionRole::Agent { descriptor, .. }) => {
                self.primaries.remove(&descriptor.agent_id);
                self.delegates.remove(&descriptor.agent_id);
                if let (Some(workspace), Some(actor)) = (workspace, actor)
                    && let Ok(event) = self.store.append_event(&EventInsert {
                        workspace: &workspace,
                        kind: WorkspaceEventKind::MemberLeft,
                        actor_id: &actor,
                        request_id: None,
                        target_id: None,
                        task_id: None,
                        content: None,
                        ok: None,
                        error: None,
                    })
                {
                    self.fanout(&event);
                }
            }
            Some(ConnectionRole::Delegate { owner_id }) => {
                if let Some(delegates) = self.delegates.get_mut(&owner_id) {
                    delegates.remove(&connection_id);
                }
            }
            _ => {}
        }
        let affected = self
            .pending
            .values()
            .filter(|pending| {
                family.contains(&pending.requester) || family.contains(&pending.recipient)
            })
            .map(|pending| {
                (
                    pending.workspace.clone(),
                    pending.request_id.clone(),
                    if family.contains(&pending.requester) {
                        RouterErrorCode::RequesterDisconnected
                    } else {
                        RouterErrorCode::TargetDisconnected
                    },
                )
            })
            .collect::<Vec<_>>();
        for (workspace, request_id, code) in affected {
            self.finish_pending_error(&workspace, &request_id, code)?;
        }
        Ok(())
    }

    fn shutdown_pending(&mut self) -> Result<(), RouterRuntimeError> {
        let pending = self
            .pending
            .values()
            .map(|value| (value.workspace.clone(), value.request_id.clone()))
            .collect::<Vec<_>>();
        for (workspace, request_id) in pending {
            self.finish_pending_error(&workspace, &request_id, RouterErrorCode::RouterRestarted)?;
        }
        Ok(())
    }

    fn send_execution_snapshot(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        agent_id: &str,
    ) -> Result<(), ActorError> {
        let (current, stop_pending) = agent_execution_fences(&self.store, agent_id)?;
        let selected = current.as_ref().or(stop_pending.as_ref());
        let Some(selected) = selected else {
            return Ok(());
        };
        let Some((workspace, attempt)) = execution_attempt(&self.store, selected.attempt_id)?
        else {
            return Err(StoreError::InvalidData.into());
        };
        let closed_attempt_id = stop_pending.as_ref().map(|fence| fence.attempt_id);
        self.send(
            connection_id,
            generation,
            &ServerMessage::TaskAttemptChanged {
                workspace,
                task_id: selected.task_id,
                attempt: Some(attempt),
                closed_attempt_id,
                current,
                stop_pending,
            },
        );
        Ok(())
    }

    fn notify_task_attempt_changed(
        &mut self,
        workspace: WorkspaceName,
        task_id: i64,
        attempt: Option<TaskAttempt>,
        closed_attempt_id: Option<Uuid>,
        agent_id: &str,
    ) -> Result<(), StoreError> {
        let (current, stop_pending) = agent_execution_fences(&self.store, agent_id)?;
        let Some(connection_id) = self.primaries.get(agent_id).copied() else {
            return Ok(());
        };
        let Some(generation) = self
            .connections
            .get(&connection_id)
            .map(|connection| connection.generation)
        else {
            return Ok(());
        };
        self.send(
            connection_id,
            generation,
            &ServerMessage::TaskAttemptChanged {
                workspace,
                task_id,
                attempt,
                closed_attempt_id,
                current,
                stop_pending,
            },
        );
        Ok(())
    }

    fn send(&mut self, connection_id: Uuid, generation: i64, message: &ServerMessage) -> bool {
        let registration_ack = matches!(
            message,
            ServerMessage::Registered { .. } | ServerMessage::RegisteredOperator { .. }
        );
        let registration_error = matches!(message, ServerMessage::Error { .. });
        let Ok(serialized) = serde_json::to_string(message) else {
            self.healthy.store(false, Ordering::Release);
            self.shutting_down = true;
            return false;
        };
        self.send_serialized(
            connection_id,
            generation,
            Utf8Bytes::from(serialized),
            registration_ack,
            registration_error,
        )
    }

    fn send_serialized(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        serialized: Utf8Bytes,
        registration_ack: bool,
        registration_error: bool,
    ) -> bool {
        let Some(connection) = self.connections.get(&connection_id) else {
            return false;
        };
        if connection.generation != generation {
            return false;
        }
        let Ok(bytes) = u32::try_from(serialized.len()) else {
            let _ = connection.writer.close.send(Some(SocketClose {
                code: 1013,
                reason: "overloaded",
            }));
            return false;
        };
        let bytes = bytes.max(1);
        let Ok(permit) = connection
            .writer
            .bytes
            .clone()
            .try_acquire_many_owned(bytes)
        else {
            let _ = connection.writer.close.send(Some(SocketClose {
                code: 1013,
                reason: "overloaded",
            }));
            return false;
        };
        if connection
            .writer
            .sender
            .try_send(OutboundFrame {
                message: Message::Text(serialized),
                registration_ack,
                registration_error,
                _bytes: permit,
            })
            .is_err()
        {
            let _ = connection.writer.close.send(Some(SocketClose {
                code: 1013,
                reason: "overloaded",
            }));
            return false;
        }
        true
    }

    fn send_error(
        &mut self,
        connection_id: Uuid,
        generation: i64,
        request_id: Option<String>,
        code: RouterErrorCode,
        workspace: Option<WorkspaceName>,
        operation_id: Option<Uuid>,
        current_version: Option<i64>,
    ) {
        self.send(
            connection_id,
            generation,
            &ServerMessage::Error {
                request_id,
                code,
                workspace,
                operation_id,
                current_version,
            },
        );
    }

    fn fanout(&mut self, event: &WorkspaceEvent) {
        let Ok(serialized) = serde_json::to_string(&ServerMessage::WorkspaceEvent {
            event: event.clone(),
        }) else {
            self.healthy.store(false, Ordering::Release);
            return;
        };
        let shared = Utf8Bytes::from(serialized);
        let recipients = self
            .subscribers
            .get(&event.workspace)
            .cloned()
            .unwrap_or_default();
        let mut failed = Vec::new();
        for recipient in recipients {
            let generation = self
                .connections
                .get(&recipient)
                .map(|value| value.generation);
            if let Some(generation) = generation
                && !self.send_serialized(recipient, generation, shared.clone(), false, false)
            {
                failed.push((recipient, generation));
            }
        }
        for (recipient, generation) in failed {
            if self.disconnect(recipient, generation).is_err() {
                self.healthy.store(false, Ordering::Release);
                self.shutting_down = true;
            }
        }
    }

    fn current_workspace(&self, connection_id: Uuid) -> Result<WorkspaceName, ActorError> {
        let effective = self
            .effective_primary(connection_id)
            .unwrap_or(connection_id);
        self.connections
            .get(&effective)
            .and_then(|connection| connection.workspace.clone())
            .ok_or_else(|| ActorError::code(RouterErrorCode::WorkspaceRequired))
    }

    fn require_workspace_match(
        &self,
        connection_id: Uuid,
        workspace: &WorkspaceName,
    ) -> Result<(), ActorError> {
        let current = self.current_workspace(connection_id)?;
        if &current == workspace {
            Ok(())
        } else {
            Err(ActorError {
                code: RouterErrorCode::WorkspaceMismatch,
                workspace: Some(current),
                operation_id: None,
                current_version: None,
            })
        }
    }

    fn authorize_workspace(
        &self,
        connection_id: Uuid,
        workspace: &WorkspaceName,
    ) -> Result<(), ActorError> {
        let (claims, admin) = self.claims(connection_id)?;
        if admin || claims.workspaces.contains(workspace) {
            Ok(())
        } else {
            Err(ActorError::code(RouterErrorCode::WorkspaceNotFound))
        }
    }

    fn claims(&self, connection_id: Uuid) -> Result<(&PublicCredentialClaims, bool), ActorError> {
        let effective = self
            .effective_primary(connection_id)
            .unwrap_or(connection_id);
        match self
            .connections
            .get(&effective)
            .and_then(|connection| connection.role.as_ref())
        {
            Some(ConnectionRole::Agent { claims, .. }) => Ok((claims, false)),
            Some(ConnectionRole::Operator { claims, admin }) => Ok((claims, *admin)),
            _ => Err(ActorError::code(RouterErrorCode::NotRegistered)),
        }
    }

    fn connection_credential_id(&self, connection_id: Uuid) -> Option<Uuid> {
        let effective = self
            .effective_primary(connection_id)
            .unwrap_or(connection_id);
        match self.connections.get(&effective)?.role.as_ref()? {
            ConnectionRole::Agent { claims, .. } | ConnectionRole::Operator { claims, .. } => {
                Some(claims.id)
            }
            ConnectionRole::Delegate { .. } => None,
        }
    }

    fn require_registered(&self, connection_id: Uuid) -> Result<(), ActorError> {
        if self
            .connections
            .get(&connection_id)
            .is_some_and(|connection| connection.role.is_some())
        {
            Ok(())
        } else {
            Err(ActorError::code(RouterErrorCode::NotRegistered))
        }
    }

    fn require_unregistered(&self, connection_id: Uuid) -> Result<(), ActorError> {
        if self
            .connections
            .get(&connection_id)
            .is_some_and(|connection| connection.role.is_none())
        {
            Ok(())
        } else {
            Err(ActorError::code(RouterErrorCode::AgentConflict))
        }
    }

    fn require_operator(&self, connection_id: Uuid) -> Result<(), ActorError> {
        if matches!(
            self.connections
                .get(&connection_id)
                .and_then(|value| value.role.as_ref()),
            Some(ConnectionRole::Operator { .. })
        ) {
            Ok(())
        } else {
            Err(ActorError::code(RouterErrorCode::PermissionDenied))
        }
    }

    fn require_admin(&self, connection_id: Uuid) -> Result<(), ActorError> {
        if matches!(
            self.connections
                .get(&connection_id)
                .and_then(|value| value.role.as_ref()),
            Some(ConnectionRole::Operator { admin: true, .. })
        ) {
            Ok(())
        } else {
            Err(ActorError::code(RouterErrorCode::PermissionDenied))
        }
    }

    fn effective_primary(&self, connection_id: Uuid) -> Option<Uuid> {
        match self.connections.get(&connection_id)?.role.as_ref()? {
            ConnectionRole::Delegate { owner_id } => self.primaries.get(owner_id).copied(),
            _ => None,
        }
    }

    fn connection_family(&self, connection_id: Uuid) -> HashSet<Uuid> {
        let mut family = HashSet::from([connection_id]);
        if let Some(ConnectionRole::Agent { descriptor, .. }) = self
            .connections
            .get(&connection_id)
            .and_then(|connection| connection.role.as_ref())
            && let Some(delegates) = self.delegates.get(&descriptor.agent_id)
        {
            family.extend(delegates.iter().copied());
        }
        family
    }

    fn actor_id(&self, connection_id: Uuid) -> Result<String, ActorError> {
        let effective = self
            .effective_primary(connection_id)
            .unwrap_or(connection_id);
        match self
            .connections
            .get(&effective)
            .and_then(|connection| connection.role.as_ref())
        {
            Some(ConnectionRole::Agent { descriptor, .. }) => Ok(descriptor.agent_id.clone()),
            Some(ConnectionRole::Operator { claims, .. }) => {
                Ok(format!("operator:{}", claims.subject))
            }
            _ => Err(ActorError::code(RouterErrorCode::NotRegistered)),
        }
    }

    fn agents_in_workspace(&self, workspace: &WorkspaceName) -> Vec<AgentDescriptor> {
        let mut agents = self
            .connections
            .values()
            .filter(|connection| connection.workspace.as_ref() == Some(workspace))
            .filter_map(|connection| match connection.role.as_ref() {
                Some(ConnectionRole::Agent { descriptor, .. }) => Some(descriptor.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        agents.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        agents
    }

    fn set_busy(&mut self, connection_id: Uuid, busy: bool) {
        let blocked = self
            .connections
            .get(&connection_id)
            .and_then(|connection| connection.role.as_ref())
            .and_then(|role| match role {
                ConnectionRole::Agent { descriptor, .. } => Some(descriptor.agent_id.as_str()),
                _ => None,
            })
            .is_some_and(|agent_id| {
                agent_has_execution_barrier(&self.store, agent_id).unwrap_or(true)
            });
        if let Some(connection) = self.connections.get_mut(&connection_id) {
            let effective_busy = busy || connection.canceling.is_some();
            if let Some(ConnectionRole::Agent { descriptor, .. }) = connection.role.as_mut() {
                descriptor.status = if effective_busy {
                    AgentStatus::Busy
                } else {
                    AgentStatus::Idle
                };
                descriptor.ready = !effective_busy
                    && !blocked
                    && connection.stored_ready
                    && connection.workspace.is_some();
            }
        }
    }

    fn sync_delegates(&mut self, owner_connection: Uuid, workspace: Option<&WorkspaceName>) {
        let owner_id = match self
            .connections
            .get(&owner_connection)
            .and_then(|connection| connection.role.as_ref())
        {
            Some(ConnectionRole::Agent { descriptor, .. }) => descriptor.agent_id.clone(),
            _ => return,
        };
        let workspace = workspace.cloned();
        for delegate in self.delegates.get(&owner_id).cloned().unwrap_or_default() {
            if let Some(connection) = self.connections.get_mut(&delegate) {
                connection.workspace.clone_from(&workspace);
            }
        }
    }

    fn notify_membership(
        &mut self,
        owner_connection: Uuid,
        workspace: Option<WorkspaceName>,
        cursor: i64,
    ) {
        let (owner_id, owner_generation) = match self.connections.get(&owner_connection) {
            Some(connection) => match connection.role.as_ref() {
                Some(ConnectionRole::Agent { descriptor, .. }) => {
                    (descriptor.agent_id.clone(), connection.generation)
                }
                Some(ConnectionRole::Operator { .. }) => {
                    self.send(
                        owner_connection,
                        connection.generation,
                        &ServerMessage::WorkspaceChanged { workspace, cursor },
                    );
                    return;
                }
                _ => return,
            },
            None => return,
        };
        self.send(
            owner_connection,
            owner_generation,
            &ServerMessage::WorkspaceChanged {
                workspace: workspace.clone(),
                cursor,
            },
        );
        for delegate in self.delegates.get(&owner_id).cloned().unwrap_or_default() {
            if let Some(generation) = self
                .connections
                .get(&delegate)
                .map(|value| value.generation)
            {
                self.send(
                    delegate,
                    generation,
                    &ServerMessage::WorkspaceChanged {
                        workspace: workspace.clone(),
                        cursor,
                    },
                );
            }
        }
    }

    fn remove_subscriptions_for_owner(&mut self, owner_connection: Uuid) {
        let owner_id = match self
            .connections
            .get(&owner_connection)
            .and_then(|connection| connection.role.as_ref())
        {
            Some(ConnectionRole::Agent { descriptor, .. }) => descriptor.agent_id.clone(),
            _ => String::new(),
        };
        let delegates = self.delegates.get(&owner_id).cloned().unwrap_or_default();
        for subscribers in self.subscribers.values_mut() {
            subscribers.remove(&owner_connection);
            for delegate in &delegates {
                subscribers.remove(delegate);
            }
        }
    }

    fn close_all(&mut self) {
        self.healthy.store(false, Ordering::Release);
        self.connections.clear();
        self.primaries.clear();
        self.delegates.clear();
        self.subscribers.clear();
    }
}

async fn run_external_worker(
    client: IntegrationClient,
    active: crate::integrations::ActiveIntegration,
    prepared: PreparedExternalOperation,
    controls: mpsc::Sender<ControlCommand>,
) {
    let payload = prepared
        .payload
        .clone()
        .expect("worker requires a prepared payload");
    let result = match payload {
        OperationPayload::Import { external_id } | OperationPayload::Link { external_id, .. } => {
            external_call(
                prepared.deadline,
                false,
                client.get_issue(&active.connection, &external_id),
            )
            .await
            .map(ExternalWorkerSuccess::Read)
            .map_or_else(
                ExternalWorkerResult::ClientError,
                ExternalWorkerResult::Success,
            )
        }
        OperationPayload::PublishIssue {
            creation_id,
            title,
            body,
            ..
        } => {
            let preflight =
                external_call(prepared.deadline, false, client.check(&active.connection)).await;
            match preflight {
                Err(error) => ExternalWorkerResult::ClientError(error),
                Ok(_) => match request_external_gate(&controls, &prepared).await {
                    Err(code) => ExternalWorkerResult::GateDenied(code),
                    Ok(()) => external_call(
                        prepared.deadline,
                        true,
                        client.create_issue(&active.connection, creation_id, &title, &body),
                    )
                    .await
                    .map(ExternalWorkerSuccess::Mutation)
                    .map_or_else(
                        ExternalWorkerResult::ClientError,
                        ExternalWorkerResult::Success,
                    ),
                },
            }
        }
        OperationPayload::PublishReport {
            creation_id,
            issue_id,
            body,
            ..
        } => {
            let preflight = external_call(
                prepared.deadline,
                false,
                client.get_issue(&active.connection, &issue_id),
            )
            .await;
            match preflight {
                Err(error) => ExternalWorkerResult::ClientError(error),
                Ok(_) => match request_external_gate(&controls, &prepared).await {
                    Err(code) => ExternalWorkerResult::GateDenied(code),
                    Ok(()) => external_call(
                        prepared.deadline,
                        true,
                        client.create_comment(&active.connection, &issue_id, creation_id, &body),
                    )
                    .await
                    .map(ExternalWorkerSuccess::Mutation)
                    .map_or_else(
                        ExternalWorkerResult::ClientError,
                        ExternalWorkerResult::Success,
                    ),
                },
            }
        }
    };
    let _ = controls
        .send(ControlCommand::ExternalFinished { prepared, result })
        .await;
}

#[allow(clippy::too_many_arguments)]
async fn run_resolution_worker(
    client: IntegrationClient,
    active: crate::integrations::ActiveIntegration,
    prepared: PreparedExternalResolution,
    connection_id: Uuid,
    generation: i64,
    request_id: String,
    controls: mpsc::Sender<ControlCommand>,
) {
    let result = match &prepared.payload {
        OperationPayload::PublishIssue { marker, .. } => {
            external_call(
                prepared.deadline,
                false,
                client.verify_issue_marker(&active.connection, &prepared.external_id, marker),
            )
            .await
        }
        OperationPayload::PublishReport {
            issue_id, marker, ..
        } => {
            external_call(
                prepared.deadline,
                false,
                client.verify_comment_marker(
                    &active.connection,
                    issue_id,
                    &prepared.external_id,
                    marker,
                ),
            )
            .await
        }
        OperationPayload::Import { .. } | OperationPayload::Link { .. } => Err(
            IntegrationClientError::Failed(ExternalErrorCode::InvalidRequest),
        ),
    };
    let _ = controls
        .send(ControlCommand::ResolutionFinished {
            connection_id,
            generation,
            request_id,
            prepared,
            result,
        })
        .await;
}

async fn external_call<T>(
    deadline: Instant,
    mutation: bool,
    call: impl Future<Output = Result<T, IntegrationClientError>>,
) -> Result<T, IntegrationClientError> {
    tokio::time::timeout_at(deadline, call)
        .await
        .unwrap_or_else(|_| {
            Err(if mutation {
                IntegrationClientError::Unconfirmed(ExternalErrorCode::Timeout)
            } else {
                IntegrationClientError::Failed(ExternalErrorCode::Timeout)
            })
        })
}

async fn request_external_gate(
    controls: &mpsc::Sender<ControlCommand>,
    prepared: &PreparedExternalOperation,
) -> Result<(), RouterErrorCode> {
    let (reply, result) = oneshot::channel();
    controls
        .send(ControlCommand::ExternalGate {
            prepared: prepared.clone(),
            reply,
        })
        .await
        .map_err(|_| RouterErrorCode::ConfigurationRequired)?;
    tokio::time::timeout_at(prepared.deadline, result)
        .await
        .map_err(|_| RouterErrorCode::RequestTimeout)?
        .map_err(|_| RouterErrorCode::ConfigurationRequired)?
}

fn integration_router_code(error: IntegrationClientError) -> RouterErrorCode {
    match error {
        IntegrationClientError::Busy => RouterErrorCode::IntegrationBusy,
        IntegrationClientError::Failed(
            crate::integrations::ExternalErrorCode::ConfigurationInvalid,
        ) => RouterErrorCode::IntegrationConfigurationInvalid,
        IntegrationClientError::Failed(_) | IntegrationClientError::Unconfirmed(_) => {
            RouterErrorCode::IntegrationError
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HistoryBudget<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    #[serde(rename = "requestId")]
    request_id: &'a str,
    workspace: &'a WorkspaceName,
    events: &'a [WorkspaceEvent],
    next_cursor: i64,
    has_more: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionBudget<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    #[serde(rename = "requestId")]
    request_id: &'a str,
    workspace: &'a WorkspaceName,
    events: &'a [WorkspaceEvent],
    next_cursor: i64,
    live: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkspaceListBudget<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    #[serde(rename = "requestId")]
    request_id: &'a str,
    workspaces: &'a [WorkspaceSummary],
    next_cursor: Option<&'a str>,
    has_more: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TaskListBudget<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    #[serde(rename = "requestId")]
    request_id: &'a str,
    workspace: &'a WorkspaceName,
    tasks: &'a [TaskSummary],
    next_cursor: i64,
    has_more: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TaskHistoryBudget<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    #[serde(rename = "requestId")]
    request_id: &'a str,
    workspace: &'a WorkspaceName,
    task_id: i64,
    events: &'a [TaskHistoryEvent],
    next_cursor: i64,
    has_more: bool,
}

fn largest_response_prefix(
    item_count: usize,
    mut encoded_size: impl FnMut(usize) -> Result<usize, ActorError>,
) -> Result<usize, ActorError> {
    if encoded_size(0)? > crate::protocol::MAX_RESPONSE_BYTES {
        return Err(ActorError::code(RouterErrorCode::MessageTooLarge));
    }
    let mut low = 0;
    let mut high = item_count;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if encoded_size(middle)? <= crate::protocol::MAX_RESPONSE_BYTES {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    if item_count > 0 && low == 0 {
        Err(ActorError::code(RouterErrorCode::MessageTooLarge))
    } else {
        Ok(low)
    }
}

fn serialized_size(value: &impl Serialize) -> Result<usize, ActorError> {
    serde_json::to_vec(value)
        .map(|encoded| encoded.len())
        .map_err(|_| ActorError::code(RouterErrorCode::StorageError))
}

fn bound_history_page(
    message_type: &'static str,
    request_id: &str,
    workspace: &WorkspaceName,
    events: &mut Vec<WorkspaceEvent>,
    input_cursor: i64,
    source_has_more: bool,
) -> Result<(i64, bool), ActorError> {
    let item_count = events.len();
    let count = largest_response_prefix(item_count, |count| {
        let next_cursor = events
            .get(count.wrapping_sub(1))
            .map_or(input_cursor, |event| event.seq);
        let has_more = source_has_more || count < item_count;
        if message_type == "workspace_subscription" {
            serialized_size(&SubscriptionBudget {
                message_type,
                request_id,
                workspace,
                events: &events[..count],
                next_cursor,
                live: !has_more,
            })
        } else {
            serialized_size(&HistoryBudget {
                message_type,
                request_id,
                workspace,
                events: &events[..count],
                next_cursor,
                has_more,
            })
        }
    })?;
    events.truncate(count);
    let next_cursor = events.last().map_or(input_cursor, |event| event.seq);
    Ok((next_cursor, source_has_more || count < item_count))
}

fn bound_task_history(
    request_id: &str,
    workspace: &WorkspaceName,
    task_id: i64,
    events: &mut Vec<TaskHistoryEvent>,
    input_cursor: i64,
    source_has_more: bool,
) -> Result<(i64, bool), ActorError> {
    let item_count = events.len();
    let count = largest_response_prefix(item_count, |count| {
        let next_cursor = events
            .get(count.wrapping_sub(1))
            .map_or(input_cursor, |event| event.seq);
        serialized_size(&TaskHistoryBudget {
            message_type: "task_history",
            request_id,
            workspace,
            task_id,
            events: &events[..count],
            next_cursor,
            has_more: source_has_more || count < item_count,
        })
    })?;
    events.truncate(count);
    let next_cursor = events.last().map_or(input_cursor, |event| event.seq);
    Ok((next_cursor, source_has_more || count < item_count))
}

fn bound_workspace_list(
    request_id: &str,
    workspaces: &mut Vec<WorkspaceSummary>,
    input_cursor: Option<&str>,
    source_has_more: bool,
) -> Result<(Option<String>, bool), ActorError> {
    let item_count = workspaces.len();
    let count = largest_response_prefix(item_count, |count| {
        let next_cursor = workspaces
            .get(count.wrapping_sub(1))
            .map(|workspace| workspace.name.as_str())
            .or(input_cursor);
        serialized_size(&WorkspaceListBudget {
            message_type: "workspaces",
            request_id,
            workspaces: &workspaces[..count],
            next_cursor,
            has_more: source_has_more || count < item_count,
        })
    })?;
    workspaces.truncate(count);
    let next_cursor = workspaces
        .last()
        .map(|workspace| workspace.name.as_str().to_owned())
        .or_else(|| input_cursor.map(str::to_owned));
    Ok((next_cursor, source_has_more || count < item_count))
}

fn bound_task_list(
    request_id: &str,
    workspace: &WorkspaceName,
    tasks: &mut Vec<TaskSummary>,
    input_cursor: i64,
    source_has_more: bool,
) -> Result<(i64, bool), ActorError> {
    let item_count = tasks.len();
    let count = largest_response_prefix(item_count, |count| {
        let next_cursor = tasks
            .get(count.wrapping_sub(1))
            .map_or(input_cursor, |task| task.id);
        serialized_size(&TaskListBudget {
            message_type: "tasks",
            request_id,
            workspace,
            tasks: &tasks[..count],
            next_cursor,
            has_more: source_has_more || count < item_count,
        })
    })?;
    tasks.truncate(count);
    let next_cursor = tasks.last().map_or(input_cursor, |task| task.id);
    Ok((next_cursor, source_has_more || count < item_count))
}

fn task_command_id(command: &TaskCommand) -> Option<i64> {
    match command {
        TaskCommand::Create { .. } => None,
        TaskCommand::Edit { task_id, .. }
        | TaskCommand::Assign { task_id, .. }
        | TaskCommand::Note { task_id, .. }
        | TaskCommand::Begin { task_id, .. }
        | TaskCommand::Checkpoint { task_id, .. }
        | TaskCommand::Pause { task_id, .. }
        | TaskCommand::Complete { task_id, .. }
        | TaskCommand::Cancel { task_id, .. }
        | TaskCommand::Reopen { task_id, .. }
        | TaskCommand::Interrupt { task_id, .. }
        | TaskCommand::ConfirmStopped { task_id, .. } => Some(*task_id),
    }
}

const fn terminal_pause_reason(code: RouterErrorCode) -> PauseReason {
    match code {
        RouterErrorCode::RequestTimeout => PauseReason::RequestTimeout,
        RouterErrorCode::RequestCancelled => PauseReason::RequestCancelled,
        RouterErrorCode::RouterRestarted => PauseReason::RouterRestarted,
        RouterErrorCode::TaskInterrupted => PauseReason::OperatorInterrupt,
        _ => PauseReason::TransportLost,
    }
}

#[derive(Debug)]
struct ActorError {
    code: RouterErrorCode,
    workspace: Option<WorkspaceName>,
    operation_id: Option<Uuid>,
    current_version: Option<i64>,
}

impl ActorError {
    const fn code(code: RouterErrorCode) -> Self {
        Self {
            code,
            workspace: None,
            operation_id: None,
            current_version: None,
        }
    }
}

impl From<StoreError> for ActorError {
    fn from(value: StoreError) -> Self {
        Self::code(value.into())
    }
}

fn bootstrap_admin(
    store: &mut RouterStore,
    data_dir: &std::path::Path,
) -> Result<(), RouterRuntimeError> {
    let directory = data_dir.join("credentials");
    let path = directory.join("admin.json");
    let count = store.credential_count()?;
    let credential = if path.exists() {
        read_credential(&path)?
    } else if count == 0 {
        let generated = CredentialFile::generate(
            CredentialRole::Operator,
            "admin".to_owned(),
            None,
            None,
            Vec::new(),
        )?;
        write_credential_atomic_no_replace(&directory, "admin.json", &generated)?;
        generated
    } else {
        return Err(RouterRuntimeError::Configuration);
    };
    if credential.role != CredentialRole::Operator
        || credential.subject != "admin"
        || !credential.workspaces.is_empty()
        || credential.agent_side.is_some()
        || credential.agent_client.is_some()
    {
        return Err(RouterRuntimeError::Configuration);
    }
    if count == 0 {
        store.insert_credential(&credential)?;
    } else {
        let stored = store
            .credential_by_id(credential.id)?
            .ok_or(RouterRuntimeError::Configuration)?;
        if stored.revoked_at.is_some()
            || stored.token_hash != credential.token_hash()
            || stored.claims.subject != "admin"
            || stored.claims.role != CredentialRole::Operator
        {
            return Err(RouterRuntimeError::Configuration);
        }
    }
    Ok(())
}
