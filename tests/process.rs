use std::{
    collections::{BTreeMap, VecDeque},
    ffi::OsString,
    fs,
    net::{Ipv4Addr, SocketAddr},
    os::unix::fs::{PermissionsExt as _, symlink},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    net::TcpListener,
    sync::Notify,
};
use uuid::Uuid;

use agent_session_router::process;

use process::{
    AdminControl, ChildSupervisor, HealthMarker, HealthProbe, NativeChildConfig,
    NativeChildSupervisor, NativeLauncher, OwnedServe, ProbeFailure, ProcessFuture,
    ProfilePublisher, ReqwestHealthProbe, RouterLaunchError, RuntimeRecord, RuntimeShareMode,
    RuntimeStore, ShareRequest, StartOptions, StartOutcome, StartupReady, StopOutcome,
    SystemTailscale, TailscaleControl, TailscaleSnapshot, child_startup_handshake,
    resolve_share_mode, tls_settings_from_environment, validate_tailscale_router_url,
};

fn private_temp() -> TempDir {
    let directory = tempfile::tempdir().expect("tempdir");
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).expect("chmod");
    directory
}

fn store(directory: &TempDir) -> RuntimeStore {
    RuntimeStore::new(directory.path().to_path_buf()).expect("runtime store")
}

fn local_record(instance_id: Uuid, port: u16) -> RuntimeRecord {
    RuntimeRecord {
        instance_id,
        control_url: format!("ws://127.0.0.1:{port}/ws"),
        share_mode: RuntimeShareMode::Local,
        advertised_url: None,
        owned_serve: None,
    }
}

fn tailscale_record(instance_id: Uuid, port: u16) -> RuntimeRecord {
    RuntimeRecord {
        instance_id,
        control_url: format!("ws://127.0.0.1:{port}/ws"),
        share_mode: RuntimeShareMode::Tailscale,
        advertised_url: None,
        owned_serve: Some(OwnedServe::loopback(instance_id, port)),
    }
}

fn marker(instance_id: Uuid) -> HealthMarker {
    HealthMarker {
        service: "agent-session-router".to_owned(),
        protocol_version: 2,
        status: "ok".to_owned(),
        instance_id,
    }
}

fn local_options(instance_id: Uuid, background: bool) -> StartOptions {
    StartOptions {
        bind: "127.0.0.1:0".parse().expect("bind"),
        share: ShareRequest::Local,
        tls: None,
        background,
        instance_id: Some(instance_id),
    }
}

fn valid_snapshot() -> TailscaleSnapshot {
    TailscaleSnapshot {
        backend_running: true,
        self_ipv4: vec![Ipv4Addr::new(100, 64, 0, 1)],
        online_peer_ipv4: vec![Ipv4Addr::new(100, 64, 0, 2)],
        tcp_forwards: BTreeMap::new(),
    }
}

struct SystemTailscaleFixture {
    executable: PathBuf,
    state: PathBuf,
    log: PathBuf,
    port: u16,
}

impl SystemTailscaleFixture {
    fn new(directory: &TempDir, port: u16) -> Self {
        let executable = directory.path().join("tailscale");
        let state = directory.path().join("serve.json");
        let log = directory.path().join("tailscale.log");
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{log}'\ncase \"$*\" in\n  'status --json') printf '%s' '{{\"BackendState\":\"Running\",\"Self\":{{\"TailscaleIPs\":[\"100.64.0.1\",\"fd7a:115c:a1e0::1\"]}},\"Peer\":{{\"peer\":{{\"Online\":true,\"TailscaleIPs\":[\"100.64.0.2\"]}}}}}}' ;;\n  'serve status --json') cat '{state}' ;;\n  'serve --bg --tcp={port} tcp://127.0.0.1:{port}') printf '%s' '{{\"TCP\":{{\"{port}\":{{\"TCPForward\":\"127.0.0.1:{port}\"}}}}}}' > '{state}' ;;\n  'serve --tcp={port} off') printf '%s' '{{\"TCP\":{{}}}}' > '{state}' ;;\n  *) exit 64 ;;\nesac\n",
            log = log.display(),
            state = state.display(),
        );
        fs::write(&executable, script).expect("write tailscale fixture");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
            .expect("tailscale fixture mode");
        let fixture = Self {
            executable,
            state,
            log,
            port,
        };
        fixture.write_mapping(None);
        fixture
    }

    fn control(&self) -> SystemTailscale {
        SystemTailscale::new(self.executable.clone()).expect("system tailscale fixture")
    }

    fn write_mapping(&self, target: Option<&str>) {
        let document = target.map_or_else(
            || serde_json::json!({"TCP": {}}),
            |target| {
                serde_json::json!({
                    "TCP": {
                        self.port.to_string(): {
                            "TCPForward": target.strip_prefix("tcp://").unwrap_or(target)
                        }
                    }
                })
            },
        );
        fs::write(
            &self.state,
            serde_json::to_vec(&document).expect("serve status JSON"),
        )
        .expect("write serve status");
    }

    fn commands(&self) -> Vec<String> {
        fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

#[derive(Default)]
struct FakeHealth {
    responses: VecDeque<Result<HealthMarker, ProbeFailure>>,
    probes: usize,
}

impl HealthProbe for FakeHealth {
    fn probe<'a>(
        &'a mut self,
        record: &'a RuntimeRecord,
    ) -> ProcessFuture<'a, Result<HealthMarker, ProbeFailure>> {
        self.probes += 1;
        let response = self
            .responses
            .pop_front()
            .unwrap_or_else(|| Ok(marker(record.instance_id)));
        Box::pin(async move { response })
    }
}

struct BlockingHealth {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl HealthProbe for BlockingHealth {
    fn probe<'a>(
        &'a mut self,
        record: &'a RuntimeRecord,
    ) -> ProcessFuture<'a, Result<HealthMarker, ProbeFailure>> {
        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        let instance_id = record.instance_id;
        Box::pin(async move {
            entered.notify_one();
            release.notified().await;
            Ok(marker(instance_id))
        })
    }
}

#[derive(Default)]
struct FakeAdmin {
    events: Arc<Mutex<Vec<String>>>,
    reject_owner: bool,
    reject_shutdown: bool,
    reject_wait: bool,
}

impl AdminControl for FakeAdmin {
    fn verify_owner<'a>(
        &'a mut self,
        _: &'a RuntimeRecord,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>> {
        self.events
            .lock()
            .expect("events")
            .push("admin.verify".into());
        let reject = self.reject_owner;
        Box::pin(async move {
            if reject {
                Err(RouterLaunchError::AdminAuthentication)
            } else {
                Ok(())
            }
        })
    }

    fn shutdown<'a>(
        &'a mut self,
        _: &'a RuntimeRecord,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>> {
        self.events
            .lock()
            .expect("events")
            .push("admin.shutdown".into());
        let reject = self.reject_shutdown;
        Box::pin(async move {
            if reject {
                Err(RouterLaunchError::AdminProtocol)
            } else {
                Ok(())
            }
        })
    }

    fn wait_stopped<'a>(
        &'a mut self,
        _: &'a RuntimeRecord,
        deadline: Duration,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>> {
        assert_eq!(deadline, process::SHUTDOWN_TIMEOUT);
        self.events
            .lock()
            .expect("events")
            .push("admin.stopped".into());
        let reject = self.reject_wait;
        Box::pin(async move {
            if reject {
                Err(RouterLaunchError::ShutdownTimeout)
            } else {
                Ok(())
            }
        })
    }
}

#[derive(Debug)]
struct TailscaleState {
    snapshot: TailscaleSnapshot,
    fail_snapshot: bool,
    fail_enable: bool,
    fail_disable: bool,
    events: Vec<String>,
    inspect_store: Option<RuntimeStore>,
}

#[derive(Clone)]
struct FakeTailscale(Arc<Mutex<TailscaleState>>);

impl FakeTailscale {
    fn new(snapshot: TailscaleSnapshot) -> Self {
        Self(Arc::new(Mutex::new(TailscaleState {
            snapshot,
            fail_snapshot: false,
            fail_enable: false,
            fail_disable: false,
            events: Vec::new(),
            inspect_store: None,
        })))
    }

    fn snapshot_value(&self) -> TailscaleSnapshot {
        self.0.lock().expect("tailscale").snapshot.clone()
    }
}

impl TailscaleControl for FakeTailscale {
    fn snapshot(&mut self) -> ProcessFuture<'_, Result<TailscaleSnapshot, RouterLaunchError>> {
        let mut state = self.0.lock().expect("tailscale");
        state.events.push("tailscale.status".into());
        let result = if state.fail_snapshot {
            Err(RouterLaunchError::TailscaleStatus)
        } else {
            Ok(state.snapshot.clone())
        };
        Box::pin(async move { result })
    }

    fn enable<'a>(
        &'a mut self,
        serve: &'a OwnedServe,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>> {
        let mut state = self.0.lock().expect("tailscale");
        state.events.push("tailscale.enable".into());
        if let Some(store) = &state.inspect_store {
            let record = store.read().expect("intent read").expect("intent exists");
            assert_eq!(record.owned_serve.as_ref(), Some(serve));
            assert!(record.advertised_url.is_none());
        }
        let result = if state.fail_enable {
            Err(RouterLaunchError::TailscaleCommand)
        } else {
            state
                .snapshot
                .tcp_forwards
                .insert(serve.port, serve.target.clone());
            Ok(())
        };
        Box::pin(async move { result })
    }

    fn disable<'a>(
        &'a mut self,
        serve: &'a OwnedServe,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>> {
        let mut state = self.0.lock().expect("tailscale");
        state.events.push("tailscale.disable".into());
        let result = if state.fail_disable {
            Err(RouterLaunchError::TailscaleCommand)
        } else if state.snapshot.tcp_forwards.get(&serve.port) == Some(&serve.target) {
            state.snapshot.tcp_forwards.remove(&serve.port);
            Ok(())
        } else {
            Err(RouterLaunchError::TailscaleMappingConflict)
        };
        Box::pin(async move { result })
    }
}

#[derive(Default)]
struct FakeProfile {
    calls: Vec<(String, RuntimeRecord)>,
    fail: bool,
}

impl ProfilePublisher for FakeProfile {
    fn publish(
        &mut self,
        router_url: &str,
        record: &RuntimeRecord,
    ) -> Result<(), RouterLaunchError> {
        self.calls.push((router_url.to_owned(), record.clone()));
        if self.fail {
            Err(RouterLaunchError::Profile)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Default)]
struct ChildState {
    launches: usize,
    acknowledgements: usize,
    terminations: usize,
    waits: usize,
    detached: bool,
    runtime_seen_before_ack: bool,
    lock_free_while_waiting: bool,
}

struct FakeChild {
    state: Arc<Mutex<ChildState>>,
    control_url: String,
    ready_instance: Option<Uuid>,
    fail_launch: bool,
    fail_ack: bool,
    wait_code: i32,
    inspect_store: Option<RuntimeStore>,
    check_lock_on_wait: bool,
}

impl FakeChild {
    fn new(control_url: impl Into<String>) -> Self {
        Self {
            state: Arc::new(Mutex::new(ChildState::default())),
            control_url: control_url.into(),
            ready_instance: None,
            fail_launch: false,
            fail_ack: false,
            wait_code: 0,
            inspect_store: None,
            check_lock_on_wait: false,
        }
    }
}

impl ChildSupervisor for FakeChild {
    fn launch(
        &mut self,
        expected_instance: Uuid,
        _: bool,
    ) -> ProcessFuture<'_, Result<StartupReady, RouterLaunchError>> {
        self.state.lock().expect("child").launches += 1;
        let result = if self.fail_launch {
            Err(RouterLaunchError::ChildLaunch)
        } else {
            Ok(StartupReady {
                instance_id: self.ready_instance.unwrap_or(expected_instance),
                control_url: self.control_url.clone(),
            })
        };
        Box::pin(async move { result })
    }

    fn acknowledge(
        &mut self,
        instance_id: Uuid,
    ) -> ProcessFuture<'_, Result<(), RouterLaunchError>> {
        let mut state = self.state.lock().expect("child");
        state.acknowledgements += 1;
        if let Some(store) = &self.inspect_store {
            state.runtime_seen_before_ack = store
                .read()
                .expect("runtime read")
                .is_some_and(|record| record.instance_id == instance_id);
        }
        let fail = self.fail_ack;
        Box::pin(async move {
            if fail {
                Err(RouterLaunchError::StartupProtocol)
            } else {
                Ok(())
            }
        })
    }

    fn terminate(&mut self) -> ProcessFuture<'_, Result<(), RouterLaunchError>> {
        self.state.lock().expect("child").terminations += 1;
        Box::pin(async { Ok(()) })
    }

    fn wait(&mut self) -> ProcessFuture<'_, Result<i32, RouterLaunchError>> {
        let mut state = self.state.lock().expect("child");
        state.waits += 1;
        if self.check_lock_on_wait {
            let lock = self
                .inspect_store
                .as_ref()
                .expect("store")
                .lock()
                .expect("launcher lock released before foreground wait");
            state.lock_free_while_waiting = true;
            drop(lock);
        }
        let code = self.wait_code;
        Box::pin(async move { Ok(code) })
    }

    fn detach(&mut self) {
        self.state.lock().expect("child").detached = true;
    }
}

fn launcher(
    runtime_store: RuntimeStore,
    health: FakeHealth,
    admin: FakeAdmin,
    tailscale: FakeTailscale,
    profiles: FakeProfile,
    child: FakeChild,
) -> NativeLauncher<FakeHealth, FakeAdmin, FakeTailscale, FakeProfile, FakeChild> {
    NativeLauncher::new(runtime_store, health, admin, tailscale, profiles, child)
}

#[test]
fn launcher_lock_is_private_nonblocking_and_never_unlinked() {
    let directory = private_temp();
    let runtime_store = store(&directory);
    let first = runtime_store.lock().expect("first lock");
    assert!(matches!(
        runtime_store.lock(),
        Err(RouterLaunchError::RouterBusy)
    ));
    let lock_path = directory.path().join("launcher.lock");
    assert_eq!(
        fs::metadata(&lock_path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    drop(first);
    drop(runtime_store.lock().expect("lock after release"));
    assert!(lock_path.exists());
    fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o400)).expect("chmod lock");
    assert!(matches!(
        runtime_store.lock(),
        Err(RouterLaunchError::Permissions)
    ));

    fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o644)).expect("chmod lock");
    assert!(matches!(
        runtime_store.lock(),
        Err(RouterLaunchError::Permissions)
    ));

    fs::remove_file(&lock_path).expect("remove bad fixture");
    let target = directory.path().join("target");
    fs::write(&target, b"").expect("target");
    symlink(&target, &lock_path).expect("symlink");
    assert!(matches!(
        runtime_store.lock(),
        Err(RouterLaunchError::Permissions)
    ));
}

#[test]
fn runtime_record_is_strict_atomic_private_and_instance_guarded() {
    let directory = private_temp();
    let runtime_store = store(&directory);
    let instance = Uuid::new_v4();
    let record = local_record(instance, 41001);
    runtime_store.write(&record).expect("write");
    assert_eq!(runtime_store.read().expect("read"), Some(record.clone()));
    assert_eq!(
        fs::metadata(runtime_store.runtime_path())
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let abandoned = directory.path().join(".asr-runtime-abandoned.tmp");
    fs::write(&abandoned, b"partial").expect("abandoned temp");
    assert_eq!(
        runtime_store.read().expect("read past abandoned temp"),
        Some(record.clone())
    );
    fs::set_permissions(
        runtime_store.runtime_path(),
        fs::Permissions::from_mode(0o400),
    )
    .expect("runtime mode");
    assert!(matches!(
        runtime_store.read(),
        Err(RouterLaunchError::Permissions)
    ));
    fs::set_permissions(
        runtime_store.runtime_path(),
        fs::Permissions::from_mode(0o600),
    )
    .expect("restore runtime mode");
    assert!(
        !runtime_store
            .remove_if_instance(Uuid::new_v4())
            .expect("guard")
    );
    assert!(runtime_store.runtime_path().exists());
    assert!(runtime_store.remove_if_instance(instance).expect("delete"));

    runtime_store.write(&record).expect("rewrite");
    let malformed = format!(
        "{{\"instanceId\":\"{instance}\",\"controlUrl\":\"ws://127.0.0.1:41001/ws\",\"shareMode\":\"local\",\"extra\":true}}"
    );
    fs::write(runtime_store.runtime_path(), malformed).expect("malformed");
    assert!(matches!(
        runtime_store.read(),
        Err(RouterLaunchError::InvalidRuntimeRecord)
    ));
    fs::remove_file(runtime_store.runtime_path()).expect("remove malformed record");
    let target = directory.path().join("runtime-target");
    fs::write(&target, b"{}").expect("runtime target");
    symlink(&target, runtime_store.runtime_path()).expect("runtime symlink");
    assert!(matches!(
        runtime_store.read(),
        Err(RouterLaunchError::Permissions)
    ));
}

#[test]
fn tls_and_share_selection_are_fail_closed() {
    let directory = private_temp();
    let certificate = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    fs::write(&certificate, b"certificate").expect("certificate");
    fs::write(&key, b"key").expect("key");
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).expect("key mode");
    let bind: SocketAddr = "0.0.0.0:443".parse().expect("bind");

    let mut partial = BTreeMap::new();
    partial.insert(
        OsString::from("ROUTER_TLS_CERT"),
        certificate.as_os_str().to_owned(),
    );
    assert!(matches!(
        tls_settings_from_environment(bind, partial.clone()),
        Err(RouterLaunchError::TlsInvalid)
    ));

    let mut complete = BTreeMap::new();
    complete.insert(
        OsString::from("ROUTER_TLS_CERT"),
        certificate.as_os_str().to_owned(),
    );
    complete.insert(OsString::from("ROUTER_TLS_KEY"), key.as_os_str().to_owned());
    complete.insert(
        OsString::from("ROUTER_PUBLIC_URL"),
        OsString::from("wss://router.example:443/ws"),
    );
    let tls = tls_settings_from_environment(bind, complete.clone())
        .expect("valid tls")
        .expect("tls selected");
    assert_eq!(tls.public_url, "wss://router.example:443/ws");

    fs::set_permissions(&key, fs::Permissions::from_mode(0o644)).expect("bad key mode");
    assert!(matches!(
        tls_settings_from_environment(bind, complete.clone()),
        Err(RouterLaunchError::TlsInvalid)
    ));
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).expect("restore key mode");

    assert!(matches!(
        tls_settings_from_environment(bind, BTreeMap::new()),
        Err(RouterLaunchError::TlsRequired)
    ));
    assert!(matches!(
        resolve_share_mode(
            ShareRequest::Tailscale,
            "127.0.0.1:0".parse().expect("bind"),
            Some(&tls),
            Some(&valid_snapshot())
        ),
        Err(RouterLaunchError::TlsInvalid)
    ));
    assert_eq!(
        resolve_share_mode(
            ShareRequest::Auto,
            "127.0.0.1:0".parse().expect("bind"),
            None,
            None
        )
        .expect("local fallback"),
        RuntimeShareMode::Local
    );
    assert_eq!(
        resolve_share_mode(
            ShareRequest::Auto,
            "127.0.0.1:0".parse().expect("bind"),
            None,
            Some(&valid_snapshot())
        )
        .expect("tailscale auto"),
        RuntimeShareMode::Tailscale
    );
    assert!(matches!(
        resolve_share_mode(
            ShareRequest::Lan,
            "127.0.0.1:0".parse().expect("bind"),
            Some(&tls),
            None
        ),
        Err(RouterLaunchError::TlsRequired)
    ));
}

#[test]
fn tailscale_url_validation_rejects_dns_plain_lan_and_offline_addresses() {
    let snapshot = valid_snapshot();
    validate_tailscale_router_url("ws://100.64.0.1:40100/ws", &snapshot).expect("self address");
    for value in [
        "ws://router.tailnet.ts.net:40100/ws",
        "ws://192.168.1.3:40100/ws",
        "ws://100.64.0.9:40100/ws",
        "wss://100.64.0.1:40100/ws",
    ] {
        assert!(
            validate_tailscale_router_url(value, &snapshot).is_err(),
            "{value}"
        );
    }
}

#[tokio::test]
async fn healthy_existing_instance_is_reused_only_after_admin_authentication() {
    let directory = private_temp();
    let runtime_store = store(&directory);
    let record = local_record(Uuid::new_v4(), 42001);
    runtime_store.write(&record).expect("runtime");
    let child = FakeChild::new("ws://127.0.0.1:42002/ws");
    let child_state = Arc::clone(&child.state);
    let mut native = launcher(
        runtime_store.clone(),
        FakeHealth::default(),
        FakeAdmin::default(),
        FakeTailscale::new(valid_snapshot()),
        FakeProfile::default(),
        child,
    );
    assert_eq!(
        native
            .start(local_options(Uuid::new_v4(), true))
            .await
            .expect("reuse"),
        StartOutcome::Reused(record)
    );
    assert_eq!(child_state.lock().expect("child").launches, 0);

    native.admin.reject_owner = true;
    assert!(matches!(
        native.start(local_options(Uuid::new_v4(), true)).await,
        Err(RouterLaunchError::AdminAuthentication)
    ));
    assert!(runtime_store.read().expect("record retained").is_some());
}

#[tokio::test]
async fn concurrent_start_start_and_start_stop_have_one_lock_owner() {
    let directory = private_temp();
    let tailscale = SystemTailscaleFixture::new(&directory, 42011);
    let runtime_store = store(&directory);
    let record = local_record(Uuid::new_v4(), 42011);
    runtime_store.write(&record).expect("runtime");
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut starting = NativeLauncher::new(
        runtime_store.clone(),
        BlockingHealth {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        },
        FakeAdmin::default(),
        tailscale.control(),
        FakeProfile::default(),
        FakeChild::new("ws://127.0.0.1:42012/ws"),
    );
    let start =
        tokio::spawn(async move { starting.start(local_options(Uuid::new_v4(), true)).await });
    entered.notified().await;

    let mut also_starting = NativeLauncher::new(
        runtime_store.clone(),
        FakeHealth::default(),
        FakeAdmin::default(),
        tailscale.control(),
        FakeProfile::default(),
        FakeChild::new("ws://127.0.0.1:42013/ws"),
    );
    assert!(matches!(
        also_starting
            .start(local_options(Uuid::new_v4(), true))
            .await,
        Err(RouterLaunchError::RouterBusy)
    ));

    let mut stopping = NativeLauncher::new(
        runtime_store,
        FakeHealth::default(),
        FakeAdmin::default(),
        tailscale.control(),
        FakeProfile::default(),
        FakeChild::new("ws://127.0.0.1:42014/ws"),
    );
    assert!(matches!(
        stopping.stop().await,
        Err(RouterLaunchError::RouterBusy)
    ));
    release.notify_one();
    assert_eq!(
        start.await.expect("start task").expect("start result"),
        StartOutcome::Reused(record)
    );
    assert!(tailscale.commands().is_empty());
}

#[tokio::test]
async fn only_connection_refused_is_stale_and_exact_serve_is_removed() {
    let directory = private_temp();
    let runtime_store = store(&directory);
    let instance = Uuid::new_v4();
    let record = tailscale_record(instance, 43001);
    runtime_store.write(&record).expect("runtime");
    let mut snapshot = valid_snapshot();
    let serve = record.owned_serve.as_ref().expect("serve");
    snapshot
        .tcp_forwards
        .insert(serve.port, serve.target.clone());
    let tailscale = FakeTailscale::new(snapshot);
    let view = tailscale.clone();
    let mut health = FakeHealth::default();
    health
        .responses
        .push_back(Err(ProbeFailure::ConnectionRefused));
    let mut native = launcher(
        runtime_store.clone(),
        health,
        FakeAdmin::default(),
        tailscale,
        FakeProfile::default(),
        FakeChild::new("ws://127.0.0.1:43002/ws"),
    );
    assert_eq!(
        native.stop().await.expect("stale stop"),
        StopOutcome::StaleRecovered
    );
    assert!(runtime_store.read().expect("runtime read").is_none());
    assert!(!view.snapshot_value().tcp_forwards.contains_key(&43001));

    for failure in [
        ProbeFailure::Tls,
        ProbeFailure::Authentication,
        ProbeFailure::MarkerMismatch,
    ] {
        runtime_store.write(&record).expect("runtime");
        let mut health = FakeHealth::default();
        health.responses.push_back(Err(failure));
        let mut snapshot = valid_snapshot();
        snapshot
            .tcp_forwards
            .insert(serve.port, serve.target.clone());
        let tailscale = FakeTailscale::new(snapshot);
        let view = tailscale.clone();
        let mut native = launcher(
            runtime_store.clone(),
            health,
            FakeAdmin::default(),
            tailscale,
            FakeProfile::default(),
            FakeChild::new("ws://127.0.0.1:43002/ws"),
        );
        assert!(
            matches!(native.stop().await, Err(RouterLaunchError::Health(actual)) if actual == failure)
        );
        assert_eq!(
            runtime_store.read().expect("record retained"),
            Some(record.clone())
        );
        assert_eq!(
            view.snapshot_value().tcp_forwards.get(&43001),
            Some(&serve.target)
        );
    }
}

#[tokio::test]
async fn stale_cleanup_preserves_foreign_mapping_and_runtime_intent() {
    let directory = private_temp();
    let runtime_store = store(&directory);
    let record = tailscale_record(Uuid::new_v4(), 43101);
    runtime_store.write(&record).expect("runtime");
    let mut snapshot = valid_snapshot();
    snapshot
        .tcp_forwards
        .insert(43101, "tcp://127.0.0.1:9".into());
    let mut health = FakeHealth::default();
    health
        .responses
        .push_back(Err(ProbeFailure::ConnectionRefused));
    let tailscale = FakeTailscale::new(snapshot);
    let view = tailscale.clone();
    let mut native = launcher(
        runtime_store.clone(),
        health,
        FakeAdmin::default(),
        tailscale,
        FakeProfile::default(),
        FakeChild::new("ws://127.0.0.1:43102/ws"),
    );
    assert!(matches!(
        native.stop().await,
        Err(RouterLaunchError::TailscaleMappingConflict)
    ));
    assert_eq!(runtime_store.read().expect("retained"), Some(record));
    assert_eq!(
        view.snapshot_value()
            .tcp_forwards
            .get(&43101)
            .map(String::as_str),
        Some("tcp://127.0.0.1:9")
    );
}

#[tokio::test]
async fn background_start_publishes_runtime_before_ack_and_detaches_after_readiness() {
    let directory = private_temp();
    let runtime_store = store(&directory);
    let instance = Uuid::new_v4();
    let mut child = FakeChild::new("ws://127.0.0.1:44001/ws");
    child.inspect_store = Some(runtime_store.clone());
    let state = Arc::clone(&child.state);
    let mut native = launcher(
        runtime_store.clone(),
        FakeHealth::default(),
        FakeAdmin::default(),
        FakeTailscale::new(valid_snapshot()),
        FakeProfile::default(),
        child,
    );
    let StartOutcome::Started(record) = native
        .start(local_options(instance, true))
        .await
        .expect("background start")
    else {
        panic!("expected started");
    };
    assert_eq!(record.instance_id, instance);
    assert_eq!(runtime_store.read().expect("runtime"), Some(record));
    let state = state.lock().expect("child");
    assert!(state.runtime_seen_before_ack);
    assert!(state.detached);
    assert_eq!(state.terminations, 0);
    assert_eq!(native.profiles.calls.len(), 1);
}

#[tokio::test]
async fn ready_ack_and_health_failures_kill_child_and_remove_only_own_record() {
    for failure in ["instance", "ack", "health"] {
        let directory = private_temp();
        let runtime_store = store(&directory);
        let instance = Uuid::new_v4();
        let mut child = FakeChild::new("ws://127.0.0.1:44101/ws");
        if failure == "instance" {
            child.ready_instance = Some(Uuid::new_v4());
        }
        if failure == "ack" {
            child.fail_ack = true;
        }
        let state = Arc::clone(&child.state);
        let mut health = FakeHealth::default();
        if failure == "health" {
            let mut wrong = marker(instance);
            wrong.protocol_version = 1;
            health.responses.push_back(Ok(wrong));
        }
        let mut native = launcher(
            runtime_store.clone(),
            health,
            FakeAdmin::default(),
            FakeTailscale::new(valid_snapshot()),
            FakeProfile::default(),
            child,
        );
        assert!(
            native.start(local_options(instance, true)).await.is_err(),
            "{failure}"
        );
        assert!(runtime_store.read().expect("runtime").is_none());
        assert!(state.lock().expect("child").terminations > 0);
    }
}

#[tokio::test]
async fn foreground_wait_releases_launcher_lock_then_reaps_and_cleans() {
    let directory = private_temp();
    let runtime_store = store(&directory);
    let instance = Uuid::new_v4();
    let mut child = FakeChild::new("ws://127.0.0.1:44201/ws");
    child.inspect_store = Some(runtime_store.clone());
    child.check_lock_on_wait = true;
    child.wait_code = 17;
    let state = Arc::clone(&child.state);
    let mut native = launcher(
        runtime_store.clone(),
        FakeHealth::default(),
        FakeAdmin::default(),
        FakeTailscale::new(valid_snapshot()),
        FakeProfile::default(),
        child,
    );
    assert!(matches!(
        native
            .start(local_options(instance, false))
            .await
            .expect("foreground"),
        StartOutcome::ForegroundExited { code: 17, .. }
    ));
    assert!(state.lock().expect("child").lock_free_while_waiting);
    assert!(runtime_store.read().expect("runtime").is_none());
}

#[tokio::test]
async fn tailscale_start_records_intent_before_serve_and_profile_failure_rolls_back() {
    let directory = private_temp();
    let runtime_store = store(&directory);
    let instance = Uuid::new_v4();
    let tailscale = FakeTailscale::new(valid_snapshot());
    tailscale.0.lock().expect("tailscale").inspect_store = Some(runtime_store.clone());
    let view = tailscale.clone();
    let profiles = FakeProfile {
        fail: true,
        ..FakeProfile::default()
    };
    let child = FakeChild::new("ws://127.0.0.1:45001/ws");
    let child_state = Arc::clone(&child.state);
    let mut options = local_options(instance, true);
    options.share = ShareRequest::Tailscale;
    let mut native = launcher(
        runtime_store.clone(),
        FakeHealth::default(),
        FakeAdmin::default(),
        tailscale,
        profiles,
        child,
    );
    assert!(matches!(
        native.start(options).await,
        Err(RouterLaunchError::Profile)
    ));
    assert!(runtime_store.read().expect("runtime").is_none());
    assert!(!view.snapshot_value().tcp_forwards.contains_key(&45001));
    let state = view.0.lock().expect("tailscale");
    let enable = state
        .events
        .iter()
        .position(|event| event == "tailscale.enable")
        .expect("enable");
    let disable = state
        .events
        .iter()
        .position(|event| event == "tailscale.disable")
        .expect("disable");
    assert!(enable < disable);
    assert!(child_state.lock().expect("child").terminations > 0);
}

#[tokio::test]
async fn tailscale_serve_failure_terminates_child_and_removes_intent() {
    let directory = private_temp();
    let runtime_store = store(&directory);
    let instance = Uuid::new_v4();
    let tailscale = FakeTailscale::new(valid_snapshot());
    tailscale.0.lock().expect("tailscale").fail_enable = true;
    let child = FakeChild::new("ws://127.0.0.1:45011/ws");
    let child_state = Arc::clone(&child.state);
    let mut options = local_options(instance, true);
    options.share = ShareRequest::Tailscale;
    let mut native = launcher(
        runtime_store.clone(),
        FakeHealth::default(),
        FakeAdmin::default(),
        tailscale,
        FakeProfile::default(),
        child,
    );
    assert!(matches!(
        native.start(options).await,
        Err(RouterLaunchError::TailscaleCommand)
    ));
    assert!(runtime_store.read().expect("runtime").is_none());
    assert!(child_state.lock().expect("child").terminations > 0);
}

#[tokio::test]
async fn tailscale_mapping_conflict_is_rejected_without_overwrite() {
    let directory = private_temp();
    let runtime_store = store(&directory);
    let mut snapshot = valid_snapshot();
    snapshot
        .tcp_forwards
        .insert(45101, "tcp://127.0.0.1:7".into());
    let tailscale = FakeTailscale::new(snapshot);
    let view = tailscale.clone();
    let child = FakeChild::new("ws://127.0.0.1:45101/ws");
    let state = Arc::clone(&child.state);
    let instance = Uuid::new_v4();
    let mut options = local_options(instance, true);
    options.share = ShareRequest::Tailscale;
    let mut native = launcher(
        runtime_store.clone(),
        FakeHealth::default(),
        FakeAdmin::default(),
        tailscale,
        FakeProfile::default(),
        child,
    );
    assert!(matches!(
        native.start(options).await,
        Err(RouterLaunchError::TailscaleMappingConflict)
    ));
    assert_eq!(
        view.snapshot_value()
            .tcp_forwards
            .get(&45101)
            .map(String::as_str),
        Some("tcp://127.0.0.1:7")
    );
    assert!(runtime_store.read().expect("runtime").is_none());
    assert!(state.lock().expect("child").terminations > 0);
}

#[tokio::test]
async fn authenticated_stop_waits_then_removes_exact_runtime_record() {
    let directory = private_temp();
    let runtime_store = store(&directory);
    let record = local_record(Uuid::new_v4(), 46001);
    runtime_store.write(&record).expect("runtime");
    let events = Arc::new(Mutex::new(Vec::new()));
    let admin = FakeAdmin {
        events: Arc::clone(&events),
        ..FakeAdmin::default()
    };
    let mut native = launcher(
        runtime_store.clone(),
        FakeHealth::default(),
        admin,
        FakeTailscale::new(valid_snapshot()),
        FakeProfile::default(),
        FakeChild::new("ws://127.0.0.1:46002/ws"),
    );
    assert_eq!(
        native.stop().await.expect("stop"),
        StopOutcome::Stopped(record)
    );
    assert_eq!(
        events.lock().expect("events").as_slice(),
        ["admin.verify", "admin.shutdown", "admin.stopped"]
    );
    assert!(runtime_store.read().expect("runtime").is_none());
}

async fn serve_http_once(status: &str, body: &str) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let status = status.to_owned();
    let body = body.to_owned();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut request = [0_u8; 2048];
        let _ = stream.read(&mut request).await;
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("response");
    });
    address
}

#[tokio::test]
async fn reqwest_health_probe_enforces_exact_v2_marker_and_classifies_failures() {
    let instance = Uuid::new_v4();
    let exact = serde_json::json!({
        "service": "agent-session-router",
        "protocolVersion": 2,
        "status": "ok",
        "instanceId": instance,
    })
    .to_string();
    let address = serve_http_once("200 OK", &exact).await;
    let record = local_record(instance, address.port());
    let mut probe = ReqwestHealthProbe::new(None);
    assert_eq!(
        probe.probe(&record).await.expect("health"),
        marker(instance)
    );

    for body in [
        serde_json::json!({"service":"other","protocolVersion":2,"status":"ok","instanceId":instance}).to_string(),
        serde_json::json!({"service":"agent-session-router","protocolVersion":1,"status":"ok","instanceId":instance}).to_string(),
        serde_json::json!({"service":"agent-session-router","protocolVersion":2,"status":"unhealthy","instanceId":instance}).to_string(),
        serde_json::json!({"service":"agent-session-router","protocolVersion":2,"status":"ok","instanceId":Uuid::new_v4()}).to_string(),
    ] {
        let address = serve_http_once("200 OK", &body).await;
        let record = local_record(instance, address.port());
        assert_eq!(probe.probe(&record).await, Err(ProbeFailure::MarkerMismatch));
    }

    let address = serve_http_once("401 Unauthorized", "{}").await;
    let record = local_record(instance, address.port());
    assert_eq!(
        probe.probe(&record).await,
        Err(ProbeFailure::Authentication)
    );

    let address = serve_http_once("200 OK", &exact).await;
    let tls_record = RuntimeRecord {
        control_url: format!("wss://127.0.0.1:{}/ws", address.port()),
        ..local_record(instance, address.port())
    };
    assert_eq!(probe.probe(&tls_record).await, Err(ProbeFailure::Tls));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    drop(listener);
    let record = local_record(instance, address.port());
    assert_eq!(
        probe.probe(&record).await,
        Err(ProbeFailure::ConnectionRefused)
    );
}

#[tokio::test]
async fn startup_handshake_requires_exact_ack_and_times_out_without_one() {
    let instance = Uuid::new_v4();
    let ready = StartupReady {
        instance_id: instance,
        control_url: "ws://127.0.0.1:47001/ws".into(),
    };
    let (parent, child) = tokio::io::duplex(8192);
    let (mut child_input, mut child_output) = tokio::io::split(child);
    let child_ready = ready.clone();
    let task = tokio::spawn(async move {
        child_startup_handshake(&mut child_output, &mut child_input, &child_ready).await
    });
    let (parent_input, mut parent_output) = tokio::io::split(parent);
    let mut parent_input = BufReader::new(parent_input);
    let mut frame = Vec::new();
    parent_input
        .read_until(b'\n', &mut frame)
        .await
        .expect("ready frame");
    assert_eq!(
        serde_json::from_slice::<StartupReady>(&frame).expect("ready"),
        ready
    );
    parent_output
        .write_all(format!("{{\"instanceId\":\"{instance}\"}}\n").as_bytes())
        .await
        .expect("ack");
    task.await.expect("join").expect("handshake");

    let (parent, child) = tokio::io::duplex(8192);
    let (mut child_input, mut child_output) = tokio::io::split(child);
    let child_ready = ready.clone();
    let task = tokio::spawn(async move {
        child_startup_handshake(&mut child_output, &mut child_input, &child_ready).await
    });
    let (parent_input, mut parent_output) = tokio::io::split(parent);
    let mut parent_input = BufReader::new(parent_input);
    let mut frame = Vec::new();
    parent_input
        .read_until(b'\n', &mut frame)
        .await
        .expect("ready frame");
    parent_output
        .write_all(format!("{{\"instanceId\":\"{}\"}}\n", Uuid::new_v4()).as_bytes())
        .await
        .expect("bad ack");
    assert!(matches!(
        task.await.expect("join"),
        Err(RouterLaunchError::StartupProtocol)
    ));

    let (parent, child) = tokio::io::duplex(8192);
    let (mut child_input, mut child_output) = tokio::io::split(child);
    let task = tokio::spawn(async move {
        child_startup_handshake(&mut child_output, &mut child_input, &ready).await
    });
    let (parent_input, _parent_output) = tokio::io::split(parent);
    let mut parent_input = BufReader::new(parent_input);
    let mut frame = Vec::new();
    parent_input
        .read_until(b'\n', &mut frame)
        .await
        .expect("ready frame");
    assert!(matches!(
        task.await.expect("join"),
        Err(RouterLaunchError::StartupTimeout)
    ));
}

#[tokio::test]
async fn native_child_supervisor_reads_ready_sends_ack_and_reaps_startup_crash() {
    let directory = private_temp();
    let stderr = directory.path().join("child.stderr");
    let instance = Uuid::new_v4();
    let ready = serde_json::to_string(&StartupReady {
        instance_id: instance,
        control_url: "ws://127.0.0.1:48001/ws".into(),
    })
    .expect("ready");
    let config = NativeChildConfig {
        program: PathBuf::from("/bin/sh"),
        arguments: vec![
            OsString::from("-c"),
            OsString::from(format!("printf '%s\\n' '{ready}'; read ack")),
        ],
        cwd: directory.path().to_path_buf(),
        environment: vec![(OsString::from("PATH"), OsString::from("/usr/bin:/bin"))],
        stderr_file: stderr.clone(),
        signal_process_group: false,
    };
    let mut child = NativeChildSupervisor::new(config);
    assert_eq!(
        child
            .launch(instance, true)
            .await
            .expect("launch")
            .instance_id,
        instance
    );
    child.acknowledge(instance).await.expect("ack");
    assert_eq!(child.wait().await.expect("wait"), 0);
    assert_eq!(
        fs::metadata(stderr).expect("stderr").permissions().mode() & 0o777,
        0o600
    );

    let crash = NativeChildConfig {
        program: PathBuf::from("/bin/sh"),
        arguments: vec![OsString::from("-c"), OsString::from("exit 7")],
        cwd: directory.path().to_path_buf(),
        environment: vec![(OsString::from("PATH"), OsString::from("/usr/bin:/bin"))],
        stderr_file: directory.path().join("crash.stderr"),
        signal_process_group: false,
    };
    let mut child = NativeChildSupervisor::new(crash);
    assert!(matches!(
        child.launch(Uuid::new_v4(), true).await,
        Err(RouterLaunchError::StartupProtocol)
    ));
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn system_tailscale_fixture_drives_launcher_conflict_cleanup_and_rollback() {
    {
        let directory = private_temp();
        let fixture = SystemTailscaleFixture::new(&directory, 49001);
        fixture.write_mapping(Some("tcp://127.0.0.1:7"));
        let runtime_store = store(&directory);
        let child = FakeChild::new("ws://127.0.0.1:49001/ws");
        let child_state = Arc::clone(&child.state);
        let instance = Uuid::new_v4();
        let mut options = local_options(instance, true);
        options.share = ShareRequest::Tailscale;
        let mut native = NativeLauncher::new(
            runtime_store.clone(),
            FakeHealth::default(),
            FakeAdmin::default(),
            fixture.control(),
            FakeProfile::default(),
            child,
        );
        assert!(matches!(
            native.start(options).await,
            Err(RouterLaunchError::TailscaleMappingConflict)
        ));
        assert!(runtime_store.read().expect("runtime").is_none());
        assert_eq!(child_state.lock().expect("child").terminations, 1);
        let commands = fixture.commands();
        assert_eq!(
            commands
                .iter()
                .filter(|command| command.as_str() == "status --json")
                .count(),
            2
        );
        assert!(
            !commands
                .iter()
                .any(|command| command.starts_with("serve --bg"))
        );
    }

    {
        let directory = private_temp();
        let fixture = SystemTailscaleFixture::new(&directory, 49002);
        let runtime_store = store(&directory);
        let child = FakeChild::new("ws://127.0.0.1:49002/ws");
        let child_state = Arc::clone(&child.state);
        let instance = Uuid::new_v4();
        let mut options = local_options(instance, true);
        options.share = ShareRequest::Tailscale;
        let mut native = NativeLauncher::new(
            runtime_store.clone(),
            FakeHealth::default(),
            FakeAdmin::default(),
            fixture.control(),
            FakeProfile {
                fail: true,
                ..FakeProfile::default()
            },
            child,
        );
        assert!(matches!(
            native.start(options).await,
            Err(RouterLaunchError::Profile)
        ));
        assert!(runtime_store.read().expect("runtime").is_none());
        assert_eq!(child_state.lock().expect("child").terminations, 1);
        let commands = fixture.commands();
        let enable = commands
            .iter()
            .position(|command| command == "serve --bg --tcp=49002 tcp://127.0.0.1:49002")
            .expect("exact serve enable");
        let disable = commands
            .iter()
            .position(|command| command == "serve --tcp=49002 off")
            .expect("exact serve cleanup");
        assert!(enable < disable);
        let status: serde_json::Value =
            serde_json::from_slice(&fs::read(&fixture.state).expect("serve state"))
                .expect("serve state JSON");
        assert_eq!(status, serde_json::json!({"TCP": {}}));
    }

    {
        let directory = private_temp();
        let fixture = SystemTailscaleFixture::new(&directory, 49003);
        let runtime_store = store(&directory);
        let instance = Uuid::new_v4();
        let record = tailscale_record(instance, 49003);
        let owned = record.owned_serve.as_ref().expect("owned serve").clone();
        runtime_store.write(&record).expect("stale runtime intent");
        fixture.write_mapping(Some(&owned.target));
        let mut health = FakeHealth::default();
        health
            .responses
            .push_back(Err(ProbeFailure::ConnectionRefused));
        let mut native = NativeLauncher::new(
            runtime_store.clone(),
            health,
            FakeAdmin::default(),
            fixture.control(),
            FakeProfile::default(),
            FakeChild::new("ws://127.0.0.1:49004/ws"),
        );
        assert_eq!(
            native.stop().await.expect("recover owned stale serve"),
            StopOutcome::StaleRecovered
        );
        assert!(runtime_store.read().expect("runtime").is_none());
        assert!(
            fixture
                .commands()
                .iter()
                .any(|command| command == "serve --tcp=49003 off")
        );
    }

    {
        let directory = private_temp();
        let fixture = SystemTailscaleFixture::new(&directory, 49004);
        let runtime_store = store(&directory);
        let record = tailscale_record(Uuid::new_v4(), 49004);
        runtime_store.write(&record).expect("stale runtime intent");
        let mut health = FakeHealth::default();
        health
            .responses
            .push_back(Err(ProbeFailure::ConnectionRefused));
        let mut native = NativeLauncher::new(
            runtime_store.clone(),
            health,
            FakeAdmin::default(),
            fixture.control(),
            FakeProfile::default(),
            FakeChild::new("ws://127.0.0.1:49005/ws"),
        );
        assert_eq!(
            native.stop().await.expect("remove stale intent"),
            StopOutcome::StaleRecovered
        );
        assert!(runtime_store.read().expect("runtime").is_none());
        assert!(
            !fixture
                .commands()
                .iter()
                .any(|command| command.starts_with("serve --tcp="))
        );
    }
}
