use std::{
    fs::{self, File},
    io::{self, Read, Write},
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::{Component, Path, PathBuf},
};

use rustix::fs::{AtFlags, Mode, OFlags, RenameFlags, open, openat, renameat_with, unlinkat};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    bootstrap::{
        install::{BundleInstallError, InstalledBundle, host_target, install_bundle},
        routes::{RouteError, VerifiedRoute, probe_routes},
    },
    cli::CliError,
    config::{self, Profile},
    install::installed_integrations_dir,
    protocol::WorkspaceName,
};

use super::{
    MAX_CA_BYTES, OnboardingProvider, OnboardingRoute, OnboardingTicket, VERSION,
    journal::{ActionStatus, ConfigurationLock, InstallAction, Journal, JournalError, Stage},
    providers,
};

const BUNDLE_RECORD: &str = "bundle.json";
const MAX_BUNDLE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_BUNDLE_ENTRIES: usize = 100_000;
const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::CLOEXEC)
    .union(OFlags::NOFOLLOW);
const FILE_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::CLOEXEC)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK);

/// An explicit allowlist: neither invitation nor credential material can be serialized.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstallReport {
    pub profile: String,
    pub provider: OnboardingProvider,
    pub server_id: Uuid,
    pub workspace: WorkspaceName,
    pub route: String,
    pub stage: Stage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<&'static str>,
    pub activation: &'static str,
    pub next_action: String,
}

/// Persisted only after a hash-verified archive was installed. A journaled digest
/// authenticates this receipt; it is not an independently trusted local manifest.
#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BundleRecord {
    version: u8,
    binary_sha256: String,
    integrations_sha256: String,
}

pub async fn install(
    ticket: &OnboardingTicket,
    provider: OnboardingProvider,
) -> Result<InstallReport, CliError> {
    ticket.validate().map_err(|_| invalid_ticket())?;
    if ticket.provider.is_some_and(|expected| expected != provider) {
        return Err(CliError::usage(
            "provider_mismatch",
            "use the provider specified by this invitation; other providers need a new invitation",
        ));
    }
    let path = configuration_path()?;
    let _lock = ConfigurationLock::acquire(&path).map_err(journal_error)?;
    match Journal::load(&path, ticket.invite_id, provider) {
        Ok(existing) => {
            // Repeated install is resume, but it must still prove the same ticket authority.
            let mut journal =
                Journal::prepare(&path, ticket, provider, &existing.state().executable)
                    .map_err(journal_error)?;
            continue_install(&path, &mut journal)
                .await
                .map_err(|error| with_resume(error, &journal))
        }
        Err(JournalError::NotFound) => {
            let credential = path
                .parent()
                .ok_or_else(invalid_configuration)?
                .join("onboarding")
                .join(ticket.invite_id.to_string())
                .join(provider.as_str())
                .join("credential.json");
            config::check_onboarding_binding(&path, ticket, provider, &credential)
                .map_err(|error| journal_error(error.into()))?;
            let route = probe_routes(
                &ticket.routes,
                ticket.server_id,
                Some(&ticket.manifest_sha256),
                &ca_directory(&path)?,
            )
            .await
            .map_err(|error| route_error(&error))?;
            let bundle = install_bundle(ticket, &route).await.map_err(bundle_error)?;
            let mut journal = Journal::prepare(&path, ticket, provider, &bundle.executable)
                .map_err(journal_error)?;
            let result = async {
                record_bundle(&mut journal, &bundle)?;
                configure(&path, &mut journal, &bundle, &route).await
            }
            .await;
            result.map_err(|error| with_resume(error, &journal))
        }
        Err(error) => Err(journal_error(error)),
    }
}

pub async fn resume(
    invite_id: Uuid,
    provider: OnboardingProvider,
) -> Result<InstallReport, CliError> {
    if invite_id.is_nil() {
        return Err(CliError::usage(
            "invalid_invite_id",
            "invite ID must not be nil",
        ));
    }
    let path = configuration_path()?;
    let _lock = ConfigurationLock::acquire(&path).map_err(journal_error)?;
    let mut journal = Journal::load(&path, invite_id, provider).map_err(journal_error)?;
    continue_install(&path, &mut journal)
        .await
        .map_err(|error| with_resume(error, &journal))
}

async fn continue_install(path: &Path, journal: &mut Journal) -> Result<InstallReport, CliError> {
    if journal.state().stage == Stage::Configured {
        verify_bundle(journal)?;
        providers::verify_recorded(path, journal).map_err(provider_error)?;
        let profile = bound_profile(path, journal)?;
        // Configured journals intentionally no longer possess an invitation token.
        return Ok(report(journal, profile.router_url, None));
    }
    let ticket = journal
        .state()
        .ticket
        .as_ref()
        .ok_or_else(invalid_ticket)?
        .clone();
    config::check_onboarding_binding(
        path,
        &ticket,
        journal.state().provider,
        &journal.state().credential_file,
    )
    .map_err(|error| journal_error(error.into()))?;
    let local = reuse_bundle(journal)?;
    let route = probe_routes(
        &ticket.routes,
        ticket.server_id,
        local.is_none().then_some(ticket.manifest_sha256.as_str()),
        &ca_directory(path)?,
    )
    .await
    .map_err(|error| route_error(&error))?;
    let bundle = if let Some(bundle) = local {
        bundle
    } else {
        // Only a crash before the trusted receipt was planned needs the archive again.
        let bundle = install_bundle(&ticket, &route)
            .await
            .map_err(bundle_error)?;
        if bundle.executable != journal.state().executable {
            return Err(bundle_conflict());
        }
        record_bundle(journal, &bundle)?;
        bundle
    };
    configure(path, journal, &bundle, &route).await
}

async fn configure(
    path: &Path,
    journal: &mut Journal,
    bundle: &InstalledBundle,
    route: &VerifiedRoute,
) -> Result<InstallReport, CliError> {
    let installer = providers::preflight(path, journal, &bundle.integrations_dir)
        .await
        .map_err(provider_error)?;
    // This is deliberately after every provider prerequisite/conflict check.
    journal.enroll(route).await.map_err(journal_error)?;
    installer.configure(journal).await.map_err(provider_error)?;
    providers::verify_recorded(path, journal).map_err(provider_error)?;
    journal.mark_configured().map_err(journal_error)?;
    Ok(report(journal, route.route.router_url.clone(), None))
}

pub async fn status(
    profile_name: &str,
    provider: OnboardingProvider,
) -> Result<InstallReport, CliError> {
    config::validate_profile_name(profile_name).map_err(|_| invalid_configuration())?;
    let path = configuration_path()?;
    let config = config::load_config(&path).map_err(|_| invalid_configuration())?;
    let profile = config
        .profiles
        .get(profile_name)
        .ok_or_else(not_configured)?;
    let binding = profile.bindings.get(&provider).ok_or_else(not_configured)?;
    // The binding points to the authoritative journal, not a session marker or
    // a search for whichever invitation happens to be newest.
    let invite_id = binding
        .credential_file
        .parent()
        .and_then(Path::parent)
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .and_then(|name| Uuid::parse_str(name).ok())
        .ok_or_else(invalid_configuration)?;
    let journal = Journal::load(&path, invite_id, provider).map_err(journal_error)?;
    let profile = bound_profile(&path, &journal)?;
    if journal.state().profile_name != profile_name {
        return Err(invalid_configuration());
    }
    if journal.state().stage == Stage::Configured {
        verify_bundle(&journal)?;
        // Never invoke provider CLIs: Claude's registry command starts a health child.
        providers::verify_recorded(&path, &journal).map_err(provider_error)?;
    }
    let routes = stored_routes(&profile)?;
    let (route, transport) = match probe_routes(
        &routes,
        journal.state().server_id,
        None,
        &ca_directory(&path)?,
    )
    .await
    {
        Ok(route) => (route.route.router_url, "reachable"),
        Err(RouteError::NoReachableRoute { .. }) => (profile.router_url, "unreachable"),
        Err(error) => return Err(route_error(&error)),
    };
    Ok(report(&journal, route, Some(transport)))
}

fn bound_profile(path: &Path, journal: &Journal) -> Result<Profile, CliError> {
    let config = config::load_config(path).map_err(|_| invalid_configuration())?;
    let state = journal.state();
    let profile = config
        .profiles
        .get(&state.profile_name)
        .ok_or_else(not_configured)?;
    if profile.server_id != Some(state.server_id)
        || profile.bindings.get(&state.provider).is_none_or(|binding| {
            binding.workspace != state.workspace || binding.credential_file != state.credential_file
        })
        || profile.routes.is_empty()
    {
        return Err(invalid_configuration());
    }
    Ok(profile.clone())
}

fn stored_routes(profile: &Profile) -> Result<Vec<OnboardingRoute>, CliError> {
    profile
        .routes
        .iter()
        .map(|route| {
            let ca_pem = route
                .ca_file
                .as_ref()
                .map(|path| {
                    let bytes = read_private_bounded(path, MAX_CA_BYTES)
                        .map_err(|_| route_error(&RouteError::InvalidCa))?;
                    String::from_utf8(bytes).map_err(|_| route_error(&RouteError::InvalidCa))
                })
                .transpose()?;
            Ok(OnboardingRoute {
                kind: route.kind,
                router_url: route.router_url.clone(),
                ca_pem,
            })
        })
        .collect()
}

fn report(journal: &Journal, route: String, transport: Option<&'static str>) -> InstallReport {
    let state = journal.state();
    let configured = state.stage == Stage::Configured;
    InstallReport {
        profile: state.profile_name.clone(),
        provider: state.provider,
        server_id: state.server_id,
        workspace: state.workspace.clone(),
        route,
        stage: state.stage,
        transport,
        activation: if configured {
            "restart_required"
        } else {
            "not_checked"
        },
        next_action: if configured {
            providers::next_action(
                state.provider,
                &state.executable,
                &state.profile_name,
                &state.workspace,
            )
        } else {
            resume_action(journal)
        },
    }
}

fn configuration_path() -> Result<PathBuf, CliError> {
    let path = config::config_path().map_err(|_| invalid_configuration())?;
    let path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map_err(|_| invalid_configuration())?
            .join(path)
    };
    if path
        .components()
        .any(|part| matches!(part, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(invalid_configuration());
    }
    Ok(path)
}

fn ca_directory(path: &Path) -> Result<PathBuf, CliError> {
    Ok(path
        .parent()
        .ok_or_else(invalid_configuration)?
        .join("onboarding/ca"))
}

fn receipt_path(journal: &Journal) -> Result<PathBuf, CliError> {
    Ok(journal
        .state()
        .credential_file
        .parent()
        .ok_or_else(invalid_configuration)?
        .join(BUNDLE_RECORD))
}

fn bundle_paths(journal: &Journal) -> Result<InstalledBundle, CliError> {
    let executable = journal.state().executable.clone();
    let prefix = executable
        .parent()
        .and_then(Path::parent)
        .ok_or_else(bundle_conflict)?;
    if executable.file_name().is_none_or(|name| name != "asr")
        || executable
            .parent()
            .and_then(Path::file_name)
            .is_none_or(|name| name != "bin")
        || prefix
            .file_name()
            .is_none_or(|name| name != journal.state().manifest_sha256.as_str())
        || prefix
            .parent()
            .and_then(Path::file_name)
            .is_none_or(|name| name != "versions")
    {
        return Err(bundle_conflict());
    }
    let integrations_dir =
        installed_integrations_dir(&executable).map_err(|_| bundle_conflict())?;
    Ok(InstalledBundle {
        executable,
        integrations_dir,
    })
}

fn record_bundle(journal: &mut Journal, bundle: &InstalledBundle) -> Result<(), CliError> {
    let expected = bundle_paths(journal)?;
    if expected.executable != bundle.executable
        || expected.integrations_dir != bundle.integrations_dir
    {
        return Err(bundle_conflict());
    }
    let record = inspect_bundle(bundle)?;
    let ticket = journal.state().ticket.as_ref().ok_or_else(invalid_ticket)?;
    let target = host_target().map_err(bundle_error)?;
    let artifact = ticket
        .artifacts
        .iter()
        .find(|artifact| artifact.target == target)
        .ok_or_else(|| bundle_error(BundleInstallError::UnsupportedTarget))?;
    if record.binary_sha256 != artifact.binary_sha256 {
        return Err(bundle_conflict());
    }
    let bytes = serde_json::to_vec(&record).map_err(|_| bundle_conflict())?;
    let path = receipt_path(journal)?;
    let action = journal
        .plan(InstallAction::File {
            path: path.clone(),
            sha256: digest(&bytes),
        })
        .map_err(journal_error)?;
    publish_receipt(&path, &bytes)?;
    journal.mark_applied(action).map_err(journal_error)
}

fn reuse_bundle(journal: &mut Journal) -> Result<Option<InstalledBundle>, CliError> {
    let path = receipt_path(journal)?;
    let Some((index, sha256)) =
        journal
            .state()
            .actions
            .iter()
            .enumerate()
            .find_map(|(index, record)| match &record.intent {
                InstallAction::File {
                    path: target,
                    sha256,
                } if target == &path => Some((index, sha256.clone())),
                _ => None,
            })
    else {
        return Ok(None);
    };
    let bundle = bundle_paths(journal)?;
    let bytes = serde_json::to_vec(&inspect_bundle(&bundle)?).map_err(|_| bundle_conflict())?;
    if digest(&bytes) != sha256 {
        return Err(bundle_conflict());
    }
    // Reconcile a crash after planning/publication but before marking applied.
    publish_receipt(&path, &bytes)?;
    if journal.state().actions[index].status != ActionStatus::Applied {
        journal.mark_applied(index).map_err(journal_error)?;
    }
    Ok(Some(bundle))
}

fn verify_bundle(journal: &Journal) -> Result<(), CliError> {
    let path = receipt_path(journal)?;
    let expected = journal
        .state()
        .actions
        .iter()
        .find_map(|record| match &record.intent {
            InstallAction::File {
                path: target,
                sha256,
            } if target == &path && record.status == ActionStatus::Applied => Some(sha256),
            _ => None,
        })
        .ok_or_else(bundle_conflict)?;
    let bytes = read_receipt(&path)?;
    if &digest(&bytes) != expected {
        return Err(bundle_conflict());
    }
    let saved: BundleRecord = serde_json::from_slice(&bytes).map_err(|_| bundle_conflict())?;
    if saved != inspect_bundle(&bundle_paths(journal)?)? {
        return Err(bundle_conflict());
    }
    Ok(())
}

fn read_receipt(path: &Path) -> Result<Vec<u8>, CliError> {
    read_private_bounded(path, 4096)
}

fn read_private_bounded(path: &Path, cap: usize) -> Result<Vec<u8>, CliError> {
    let parent = open_directory(path.parent().ok_or_else(bundle_conflict)?)?;
    let file = File::from(
        openat(
            &parent,
            path.file_name().ok_or_else(bundle_conflict)?,
            FILE_FLAGS,
            Mode::empty(),
        )
        .map_err(|_| bundle_conflict())?,
    );
    let metadata = file.metadata().map_err(|_| bundle_conflict())?;
    check_file(&metadata)?;
    if metadata.mode() & 0o077 != 0
        || parent.metadata().map_err(|_| bundle_conflict())?.mode() & 0o077 != 0
        || metadata.len() > cap as u64
    {
        return Err(bundle_conflict());
    }
    let mut bytes = Vec::new();
    file.take((cap + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| bundle_conflict())?;
    if bytes.len() > cap {
        return Err(bundle_conflict());
    }
    Ok(bytes)
}

fn publish_receipt(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
    let parent = open_directory(path.parent().ok_or_else(bundle_conflict)?)?;
    let name = path.file_name().ok_or_else(bundle_conflict)?;
    match openat(&parent, name, FILE_FLAGS, Mode::empty()) {
        Ok(_) => {
            if read_receipt(path)? != bytes {
                return Err(bundle_conflict());
            }
            parent.sync_all().map_err(|_| bundle_conflict())?;
            return Ok(());
        }
        Err(rustix::io::Errno::NOENT) => {}
        Err(_) => return Err(bundle_conflict()),
    }
    let temporary = format!(".bundle-{}.tmp", Uuid::new_v4());
    let mut file = File::from(
        openat(
            &parent,
            temporary.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|_| bundle_conflict())?,
    );
    let result = (|| {
        file.write_all(bytes).map_err(|_| bundle_conflict())?;
        file.sync_all().map_err(|_| bundle_conflict())?;
        renameat_with(
            &parent,
            temporary.as_str(),
            &parent,
            name,
            RenameFlags::NOREPLACE,
        )
        .map_err(|_| bundle_conflict())?;
        parent.sync_all().map_err(|_| bundle_conflict())?;
        if read_receipt(path)? != bytes {
            return Err(bundle_conflict());
        }
        Ok(())
    })();
    let _ = unlinkat(&parent, temporary.as_str(), AtFlags::empty());
    result
}

fn inspect_bundle(bundle: &InstalledBundle) -> Result<BundleRecord, CliError> {
    let parent = open_directory(bundle.executable.parent().ok_or_else(bundle_conflict)?)?;
    let mut binary = File::from(
        openat(&parent, "asr", FILE_FLAGS, Mode::empty()).map_err(|_| bundle_conflict())?,
    );
    let metadata = binary.metadata().map_err(|_| bundle_conflict())?;
    check_file(&metadata)?;
    if metadata.mode() & 0o111 == 0 {
        return Err(bundle_conflict());
    }
    let mut budget = MAX_BUNDLE_BYTES;
    let binary_sha256 = hash_file(&mut binary, metadata.len(), &mut budget)?;
    let mut hash = Sha256::new();
    let mut entries = 0;
    hash_tree(
        &bundle.integrations_dir,
        &open_directory(&bundle.integrations_dir)?,
        &mut hash,
        &mut budget,
        &mut entries,
        0,
    )?;
    Ok(BundleRecord {
        version: VERSION,
        binary_sha256,
        integrations_sha256: format!("{:x}", hash.finalize()),
    })
}

fn hash_tree(
    path: &Path,
    directory: &File,
    hash: &mut Sha256,
    budget: &mut u64,
    count: &mut usize,
    depth: usize,
) -> Result<(), CliError> {
    if depth > 128 {
        return Err(bundle_conflict());
    }
    let mut names = Vec::new();
    for entry in fs::read_dir(path).map_err(|_| bundle_conflict())? {
        *count += 1;
        if *count > MAX_BUNDLE_ENTRIES {
            return Err(bundle_conflict());
        }
        names.push(entry.map_err(|_| bundle_conflict())?.file_name());
    }
    names.sort();
    hash.update((names.len() as u64).to_le_bytes());
    for name in names {
        let mut file = File::from(
            openat(directory, &name, FILE_FLAGS, Mode::empty()).map_err(|_| bundle_conflict())?,
        );
        let metadata = file.metadata().map_err(|_| bundle_conflict())?;
        check_owner(&metadata)?;
        hash.update((name.as_bytes().len() as u64).to_le_bytes());
        hash.update(name.as_bytes());
        hash.update((metadata.mode() & 0o777).to_le_bytes());
        if metadata.is_dir() {
            hash.update(b"directory");
            hash_tree(&path.join(name), &file, hash, budget, count, depth + 1)?;
        } else {
            check_file(&metadata)?;
            hash.update(b"file");
            hash.update(hash_file(&mut file, metadata.len(), budget)?.as_bytes());
        }
    }
    Ok(())
}

fn hash_file(file: &mut File, length: u64, budget: &mut u64) -> Result<String, CliError> {
    *budget = budget.checked_sub(length).ok_or_else(bundle_conflict)?;
    let mut hash = Sha256::new();
    let copied = io::copy(&mut file.take(length + 1), &mut hash).map_err(|_| bundle_conflict())?;
    if copied != length {
        return Err(bundle_conflict());
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn open_directory(path: &Path) -> Result<File, CliError> {
    if !path.is_absolute() {
        return Err(bundle_conflict());
    }
    let mut directory =
        File::from(open("/", DIRECTORY_FLAGS, Mode::empty()).map_err(|_| bundle_conflict())?);
    for part in path.components() {
        match part {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(name) => {
                directory = File::from(
                    openat(&directory, name, DIRECTORY_FLAGS, Mode::empty())
                        .map_err(|_| bundle_conflict())?,
                );
                let metadata = directory.metadata().map_err(|_| bundle_conflict())?;
                let owner = metadata.uid();
                if (owner != 0 && owner != rustix::process::getuid().as_raw())
                    || (metadata.mode() & 0o022 != 0
                        && !(owner == 0 && metadata.mode() & 0o1000 != 0))
                {
                    return Err(bundle_conflict());
                }
            }
            _ => return Err(bundle_conflict()),
        }
    }
    check_owner(&directory.metadata().map_err(|_| bundle_conflict())?)?;
    Ok(directory)
}

fn check_owner(metadata: &fs::Metadata) -> Result<(), CliError> {
    if metadata.uid() != rustix::process::getuid().as_raw() || metadata.mode() & 0o6022 != 0 {
        return Err(bundle_conflict());
    }
    Ok(())
}

fn check_file(metadata: &fs::Metadata) -> Result<(), CliError> {
    check_owner(metadata)?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(bundle_conflict());
    }
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn invalid_ticket() -> CliError {
    CliError::usage(
        "invalid_ticket",
        "provide the complete invitation JSON on stdin",
    )
}
fn invalid_configuration() -> CliError {
    CliError::runtime(
        "invalid_configuration",
        "the saved onboarding configuration is invalid",
    )
}
fn not_configured() -> CliError {
    CliError::runtime(
        "onboarding_not_configured",
        "this profile has no onboarding binding for the selected provider",
    )
}
fn bundle_conflict() -> CliError {
    CliError::runtime(
        "bootstrap_install_conflict",
        "the installed distribution or its private receipt is missing, unsafe, or changed; restore the original installation before resuming",
    )
}
fn route_error(error: &RouteError) -> CliError {
    CliError::runtime(error.code(), error.to_string())
}
fn bundle_error(error: BundleInstallError) -> CliError {
    let guidance = match error {
        BundleInstallError::UnsupportedTarget => {
            "this OS/architecture needs a supported bootstrap artifact from the server operator"
        }
        BundleInstallError::HomeUnavailable => "set HOME to your existing private home directory",
        _ => {
            "restore the trusted bootstrap distribution or resolve the local installation conflict; no invitation was exchanged"
        }
    };
    CliError::runtime(error.code(), guidance)
}
fn journal_error(error: JournalError) -> CliError {
    let guidance = match error {
        JournalError::ConfigurationBusy => {
            "another onboarding command holds the configuration lock; retry after it finishes"
        }
        JournalError::NotFound => {
            "no journal exists for this invitation and provider; run onboarding install with the original invitation"
        }
        JournalError::InviteUnavailable => {
            "the invitation or credential is unavailable; ask the server operator to inspect revocation before requesting another invitation"
        }
        JournalError::ProfileConflict | JournalError::BindingConflict => {
            "existing profile or provider identity conflicts; retain it and ask the server operator for a distinct profile invitation"
        }
        JournalError::Unavailable | JournalError::RateLimited => {
            "enrollment could not be confirmed; resume the same invitation without creating another identity"
        }
        _ => {
            "onboarding state could not be safely updated; preserve the journal and resolve its configuration or permissions"
        }
    };
    CliError::runtime(error.code(), guidance)
}
fn provider_error(error: providers::ProviderInstallError) -> CliError {
    CliError::runtime(error.code(), error.next_action())
}
fn resume_action(journal: &Journal) -> String {
    let state = journal.state();
    let executable = state.executable.to_string_lossy().replace('\'', "'\\''");
    format!(
        "'{executable}' onboarding resume {} --provider {}",
        state.invite_id,
        state.provider.as_str()
    )
}
fn with_resume(mut error: CliError, journal: &Journal) -> CliError {
    error
        .message
        .push_str("; preserve this invitation's journal and resume with: ");
    error.message.push_str(&resume_action(journal));
    error
}
