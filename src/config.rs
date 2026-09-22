use std::{
    collections::BTreeMap,
    env,
    ffi::OsStr,
    fs::{self, File},
    io::{self, Read, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Component, Path, PathBuf},
};

use rustix::fs::{AtFlags, Mode, OFlags, linkat, mkdirat, open, openat, renameat, unlinkat};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use crate::{
    bootstrap::routes::persist_ca,
    onboarding::{
        OnboardingProvider, OnboardingRoute, OnboardingTicket, RouteKind, validate_routes,
    },
    protocol::{RouterErrorCode, WorkspaceName},
};

pub const DEFAULT_ROUTER_URL: &str = "ws://127.0.0.1:8787/ws";
pub const DEFAULT_PROFILE: &str = "local";
pub const CONFIG_VERSION: u8 = 2;
pub const DELEGATE_CONTEXT_VERSION: u8 = 1;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigFile {
    pub version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_profile: Option<String>,
    #[serde(default)]
    pub profiles: BTreeMap<String, Profile>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Profile {
    pub router_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<StoredOnboardingRoute>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub bindings: BTreeMap<OnboardingProvider, ProviderBinding>,
}

impl Profile {
    #[must_use]
    pub fn manual(router_url: String) -> Self {
        Self {
            router_url,
            server_id: None,
            routes: Vec::new(),
            bindings: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StoredOnboardingRoute {
    pub kind: RouteKind,
    pub router_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderBinding {
    pub credential_file: PathBuf,
    pub workspace: WorkspaceName,
}

#[derive(Clone, Debug)]
pub struct ProviderSelection {
    pub selection: Selection,
    pub routes: Vec<StoredOnboardingRoute>,
    pub ca_file: Option<PathBuf>,
    pub initial_workspace: Option<WorkspaceName>,
    pub expected_server_id: Option<Uuid>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DelegateLaunchContext {
    pub version: u8,
    pub router_url: String,
    pub owner_id: String,
    pub delegation_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<String>,
}

impl DelegateLaunchContext {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.version != DELEGATE_CONTEXT_VERSION
            || self.delegation_token.len() < 32
            || self.delegation_token.len() > 256
        {
            return Err(ConfigError::InvalidDelegateContext);
        }
        normalize_router_url(&self.router_url)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Selection {
    pub profile: Option<String>,
    pub router_url: Url,
    pub credential_file: Option<PathBuf>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("configuration_required")]
    Required,
    #[error("invalid_configuration")]
    Invalid,
    #[error("invalid_router_url")]
    InvalidUrl,
    #[error("invalid_profile")]
    InvalidProfile,
    #[error("invalid_delegate_context")]
    InvalidDelegateContext,
    #[error("profile_conflict")]
    ProfileConflict,
    #[error("binding_conflict")]
    BindingConflict,
    #[error("configuration_io")]
    Io(#[from] io::Error),
}

impl From<ConfigError> for RouterErrorCode {
    fn from(value: ConfigError) -> Self {
        match value {
            ConfigError::Required => Self::ConfigurationRequired,
            ConfigError::Io(_) => Self::StorageError,
            ConfigError::Invalid
            | ConfigError::InvalidUrl
            | ConfigError::InvalidProfile
            | ConfigError::InvalidDelegateContext
            | ConfigError::ProfileConflict
            | ConfigError::BindingConflict => Self::InvalidMessage,
        }
    }
}

#[must_use]
pub fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

pub fn data_dir() -> Result<PathBuf, ConfigError> {
    if let Some(value) = env::var_os("ASR_DATA_DIR").filter(|value| !value.is_empty()) {
        return expand_home(&value);
    }
    if let Some(value) = env::var_os("XDG_DATA_HOME").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(value).join("agent-session-router"));
    }
    home_dir()
        .map(|path| path.join(".local/share/agent-session-router"))
        .ok_or(ConfigError::Required)
}

pub fn config_path() -> Result<PathBuf, ConfigError> {
    if let Some(value) = env::var_os("ASR_CONFIG_PATH").filter(|value| !value.is_empty()) {
        return expand_home(&value);
    }
    if let Some(value) = env::var_os("XDG_CONFIG_HOME").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(value).join("agent-session-router/config.json"));
    }
    home_dir()
        .map(|path| path.join(".config/agent-session-router/config.json"))
        .ok_or(ConfigError::Required)
}

pub fn load_config(path: &Path) -> Result<ConfigFile, ConfigError> {
    let result = (|| {
        let parent = open_config_parent(path, false)?;
        let file = openat(
            &parent,
            path.file_name().ok_or(ConfigError::Invalid)?,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map(File::from)
        .map_err(io::Error::from)?;
        if !file.metadata()?.is_file() {
            return Err(ConfigError::Invalid);
        }
        let mut bytes = Vec::new();
        file.take(256 * 1024 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > 256 * 1024 {
            return Err(ConfigError::Invalid);
        }
        let config: ConfigFile =
            serde_json::from_slice(&bytes).map_err(|_| ConfigError::Invalid)?;
        validate_config(&config)?;
        Ok(config)
    })();
    match result {
        Err(ConfigError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(ConfigFile {
            version: CONFIG_VERSION,
            ..ConfigFile::default()
        }),
        other => other,
    }
}

pub fn save_config(path: &Path, config: &ConfigFile) -> Result<(), ConfigError> {
    validate_config(config)?;
    let parent = open_config_parent(path, true)?;
    let metadata = parent.metadata()?;
    if metadata.uid() != rustix::process::getuid().as_raw() {
        return Err(ConfigError::Invalid);
    }
    parent.set_permissions(fs::Permissions::from_mode(0o700))?;
    let name = path.file_name().ok_or(ConfigError::Invalid)?;
    let existing = config_file_identity(&parent, name)?;
    let mut current = config.clone();
    current.version = CONFIG_VERSION;
    let payload = serde_json::to_vec(&current).map_err(|_| ConfigError::Invalid)?;
    if payload.len() + 1 > 256 * 1024 {
        return Err(ConfigError::Invalid);
    }
    let temporary = format!(".config-{}.tmp", Uuid::new_v4());
    let fd = openat(
        &parent,
        temporary.as_str(),
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(io::Error::from)?;
    let result = (|| {
        let mut file = File::from(fd);
        file.write_all(&payload)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        if config_file_identity(&parent, name)? != existing {
            return Err(ConfigError::Invalid);
        }
        if existing.is_some() {
            renameat(&parent, temporary.as_str(), &parent, name).map_err(io::Error::from)?;
        } else {
            linkat(&parent, temporary.as_str(), &parent, name, AtFlags::empty())
                .map_err(io::Error::from)?;
            unlinkat(&parent, temporary.as_str(), AtFlags::empty()).map_err(io::Error::from)?;
        }
        parent.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = unlinkat(&parent, temporary.as_str(), AtFlags::empty());
    }
    result
}

fn config_file_identity(parent: &File, name: &OsStr) -> Result<Option<(u64, u64)>, ConfigError> {
    match openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(fd) => {
            let metadata = File::from(fd).metadata()?;
            if !metadata.is_file()
                || metadata.uid() != rustix::process::getuid().as_raw()
                || metadata.nlink() != 1
            {
                return Err(ConfigError::Invalid);
            }
            Ok(Some((metadata.dev(), metadata.ino())))
        }
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(error) => Err(ConfigError::Io(error.into())),
    }
}

fn open_config_parent(path: &Path, create: bool) -> Result<File, ConfigError> {
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW;
    let mut directory = File::from(
        open(
            if path.is_absolute() { "/" } else { "." },
            flags,
            Mode::empty(),
        )
        .map_err(io::Error::from)?,
    );
    for component in path.parent().ok_or(ConfigError::Invalid)?.components() {
        let name = match component {
            Component::Normal(name) => name,
            Component::RootDir | Component::CurDir => continue,
            Component::ParentDir | Component::Prefix(_) => return Err(ConfigError::Invalid),
        };
        let fd = match openat(&directory, name, flags, Mode::empty()) {
            Ok(fd) => fd,
            Err(rustix::io::Errno::NOENT) if create => {
                match mkdirat(&directory, name, Mode::RUSR | Mode::WUSR | Mode::XUSR) {
                    Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                    Err(error) => return Err(ConfigError::Io(error.into())),
                }
                directory.sync_all()?;
                openat(&directory, name, flags, Mode::empty()).map_err(io::Error::from)?
            }
            Err(error) => return Err(ConfigError::Io(error.into())),
        };
        directory = File::from(fd);
    }
    Ok(directory)
}

pub fn select(
    explicit_profile: Option<&str>,
    explicit_credential: Option<&Path>,
) -> Result<Selection, ConfigError> {
    let path = config_path()?;
    let config = load_config(&path)?;
    select_from_config(&config, explicit_profile, explicit_credential)
}

fn select_from_config(
    config: &ConfigFile,
    explicit_profile: Option<&str>,
    explicit_credential: Option<&Path>,
) -> Result<Selection, ConfigError> {
    let environment_url = env::var("ROUTER_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let (profile, raw_url) = if let Some(profile) = explicit_profile {
        let raw = profile_url(config, profile)?;
        (Some(profile.to_owned()), raw)
    } else if let Some(raw) = environment_url {
        (None, raw)
    } else if let Some(profile) = &config.default_profile {
        let raw = profile_url(config, profile)?;
        (Some(profile.clone()), raw)
    } else {
        (
            Some(DEFAULT_PROFILE.to_owned()),
            DEFAULT_ROUTER_URL.to_owned(),
        )
    };
    let credential_file = explicit_credential.map(Path::to_path_buf).or_else(|| {
        env::var_os("ASR_CREDENTIAL_FILE")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    });
    Ok(Selection {
        profile,
        router_url: normalize_router_url(&raw_url)?,
        credential_file,
    })
}

pub fn select_provider(
    explicit_profile: Option<&str>,
    explicit_credential: Option<&Path>,
    provider: OnboardingProvider,
) -> Result<ProviderSelection, ConfigError> {
    let config = load_config(&config_path()?)?;
    let selection = select_from_config(&config, explicit_profile, explicit_credential)?;
    let profile = selection
        .profile
        .as_ref()
        .and_then(|name| config.profiles.get(name));
    let mut result = ProviderSelection {
        selection,
        routes: Vec::new(),
        ca_file: env::var_os("ASR_CA_FILE")
            .filter(|value| !value.is_empty())
            .map(|value| expand_home(&value))
            .transpose()?,
        initial_workspace: None,
        expected_server_id: None,
    };
    if let Some(profile) = profile {
        result.routes.clone_from(&profile.routes);
        result.expected_server_id = profile.server_id;
        if let Some(binding) = profile.bindings.get(&provider) {
            if result.selection.credential_file.is_none() {
                result.selection.credential_file = Some(binding.credential_file.clone());
            }
            result.initial_workspace = Some(binding.workspace.clone());
        }
        if result.ca_file.is_none() {
            result.ca_file = profile
                .routes
                .iter()
                .find(|route| {
                    normalize_router_url(&route.router_url).ok().as_ref()
                        == Some(&result.selection.router_url)
                })
                .and_then(|route| route.ca_file.clone());
        }
    }
    Ok(result)
}

pub fn store_onboarding_routes(
    routes: &[OnboardingRoute],
    ca_directory: &Path,
) -> Result<Vec<StoredOnboardingRoute>, ConfigError> {
    validate_routes(routes).map_err(|_| ConfigError::Invalid)?;
    validate_stored_path(&ca_directory.join("certificate.pem"))?;
    routes
        .iter()
        .map(|route| {
            Ok(StoredOnboardingRoute {
                kind: route.kind,
                router_url: normalize_router_url(&route.router_url)?.to_string(),
                ca_file: route
                    .ca_pem
                    .as_deref()
                    .map(|pem| persist_ca(ca_directory, pem).map_err(|_| ConfigError::Invalid))
                    .transpose()?,
            })
        })
        .collect()
}

/// The caller holds the configuration lock through exchange and publication.
pub fn check_onboarding_binding(
    path: &Path,
    ticket: &OnboardingTicket,
    provider: OnboardingProvider,
    credential_file: &Path,
) -> Result<(), ConfigError> {
    ticket.validate().map_err(|_| ConfigError::Invalid)?;
    if ticket.provider.is_some_and(|expected| expected != provider) {
        return Err(ConfigError::BindingConflict);
    }
    validate_stored_path(credential_file)?;
    let config = load_config(path)?;
    check_binding(&config, ticket, provider, credential_file)?;
    let parent = open_config_parent(path, true)?;
    if parent.metadata()?.uid() != rustix::process::getuid().as_raw() {
        return Err(ConfigError::Invalid);
    }
    config_file_identity(&parent, path.file_name().ok_or(ConfigError::Invalid)?)?;
    // Exercise the same directory publication requires before consuming an invite.
    let temporary = format!(".config-preflight-{}.tmp", Uuid::new_v4());
    let fd = openat(
        &parent,
        temporary.as_str(),
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(io::Error::from)?;
    let synced = File::from(fd).sync_all();
    let removed = unlinkat(&parent, temporary.as_str(), AtFlags::empty());
    synced?;
    removed.map_err(io::Error::from)?;
    parent.sync_all()?;
    Ok(())
}

fn check_binding(
    config: &ConfigFile,
    ticket: &OnboardingTicket,
    provider: OnboardingProvider,
    credential_file: &Path,
) -> Result<(), ConfigError> {
    if let Some(profile) = config.profiles.get(&ticket.profile_name) {
        if profile.server_id != Some(ticket.server_id) {
            return Err(ConfigError::ProfileConflict);
        }
        if profile.bindings.get(&provider).is_some_and(|binding| {
            binding.credential_file != credential_file || binding.workspace != ticket.workspace
        }) {
            return Err(ConfigError::BindingConflict);
        }
    }
    Ok(())
}

/// The caller holds the configuration lock; unrelated bindings are retained.
pub fn publish_onboarding_binding(
    path: &Path,
    ticket: &OnboardingTicket,
    provider: OnboardingProvider,
    credential_file: &Path,
    router_url: &str,
    routes: Vec<StoredOnboardingRoute>,
) -> Result<(), ConfigError> {
    check_onboarding_binding(path, ticket, provider, credential_file)?;
    validate_stored_routes(&routes)?;
    let router_url = normalize_router_url(router_url)?.to_string();
    if routes.len() != ticket.routes.len()
        || !routes.iter().any(|route| route.router_url == router_url)
    {
        return Err(ConfigError::Invalid);
    }
    for route in &routes {
        if !ticket.routes.iter().any(|candidate| {
            candidate.kind == route.kind
                && normalize_router_url(&candidate.router_url)
                    .is_ok_and(|url| url.as_str() == route.router_url)
                && candidate.ca_pem.is_some() == route.ca_file.is_some()
        }) {
            return Err(ConfigError::Invalid);
        }
    }
    let mut config = load_config(path)?;
    check_binding(&config, ticket, provider, credential_file)?;
    let profile = config
        .profiles
        .entry(ticket.profile_name.clone())
        .or_insert_with(|| Profile::manual(router_url.clone()));
    profile.server_id = Some(ticket.server_id);
    profile.router_url = router_url;
    profile.routes = routes;
    profile.bindings.insert(
        provider,
        ProviderBinding {
            credential_file: credential_file.to_path_buf(),
            workspace: ticket.workspace.clone(),
        },
    );
    save_config(path, &config)
}

pub fn normalize_router_url(raw: &str) -> Result<Url, ConfigError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.chars().any(char::is_whitespace) {
        return Err(ConfigError::InvalidUrl);
    }
    let candidate = if trimmed.contains("://") {
        trimmed.to_owned()
    } else {
        format!("ws://{trimmed}")
    };
    let mut url = Url::parse(&candidate).map_err(|_| ConfigError::InvalidUrl)?;
    if !matches!(url.scheme(), "ws" | "wss")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.host_str().is_none()
    {
        return Err(ConfigError::InvalidUrl);
    }
    if url.port().is_none() && url.port_or_known_default().is_none() {
        url.set_port(Some(8787))
            .map_err(|()| ConfigError::InvalidUrl)?;
    }
    if url.path().is_empty() || url.path() == "/" {
        url.set_path("/ws");
    }
    Ok(url)
}

pub fn validate_profile_name(name: &str) -> Result<(), ConfigError> {
    if name.is_empty() || name.len() > 64 || !name.is_ascii() {
        return Err(ConfigError::InvalidProfile);
    }
    let valid = name.bytes().enumerate().all(|(index, byte)| {
        byte.is_ascii_alphanumeric() || (index > 0 && matches!(byte, b'.' | b'_' | b'-'))
    });
    if valid {
        Ok(())
    } else {
        Err(ConfigError::InvalidProfile)
    }
}

pub fn validate_workspace_argument(value: &str) -> Result<WorkspaceName, ConfigError> {
    WorkspaceName::parse(value).map_err(|_| ConfigError::Invalid)
}

fn validate_config(config: &ConfigFile) -> Result<(), ConfigError> {
    if !matches!(config.version, 1 | CONFIG_VERSION) {
        return Err(ConfigError::Invalid);
    }
    if let Some(default) = &config.default_profile {
        validate_profile_name(default)?;
        if default != DEFAULT_PROFILE && !config.profiles.contains_key(default) {
            return Err(ConfigError::Invalid);
        }
    }
    for (name, profile) in &config.profiles {
        validate_profile_name(name)?;
        if name == DEFAULT_PROFILE {
            return Err(ConfigError::Invalid);
        }
        normalize_router_url(&profile.router_url)?;
        if profile.server_id.is_some_and(|id| id.is_nil())
            || (profile.server_id.is_none()
                && (!profile.routes.is_empty() || !profile.bindings.is_empty()))
        {
            return Err(ConfigError::Invalid);
        }
        if profile.server_id.is_some() {
            validate_stored_routes(&profile.routes)?;
            if !profile.routes.iter().any(|route| {
                normalize_router_url(&route.router_url).ok()
                    == normalize_router_url(&profile.router_url).ok()
            }) {
                return Err(ConfigError::Invalid);
            }
        }
        for binding in profile.bindings.values() {
            validate_stored_path(&binding.credential_file)?;
        }
    }
    Ok(())
}

fn validate_stored_routes(routes: &[StoredOnboardingRoute]) -> Result<(), ConfigError> {
    let candidates = routes
        .iter()
        .map(|route| {
            let url = normalize_router_url(&route.router_url)?;
            if let Some(path) = &route.ca_file {
                if url.scheme() != "wss" {
                    return Err(ConfigError::Invalid);
                }
                validate_stored_path(path)?;
            }
            Ok(OnboardingRoute {
                kind: route.kind,
                router_url: url.to_string(),
                ca_pem: None,
            })
        })
        .collect::<Result<Vec<_>, ConfigError>>()?;
    validate_routes(&candidates).map_err(|_| ConfigError::Invalid)
}

fn validate_stored_path(path: &Path) -> Result<(), ConfigError> {
    if !path.is_absolute()
        || path.file_name().is_none()
        || path
            .to_str()
            .is_none_or(|value| value.chars().any(char::is_control))
    {
        return Err(ConfigError::Invalid);
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::Normal(_) => current.push(component),
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                return Err(ConfigError::Invalid);
            }
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata)
                if metadata.file_type().is_symlink()
                    || (current != path && !metadata.is_dir())
                    || (current == path && !metadata.is_file()) =>
            {
                return Err(ConfigError::Invalid);
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(ConfigError::Io(error)),
        }
    }
    Ok(())
}

fn profile_url(config: &ConfigFile, name: &str) -> Result<String, ConfigError> {
    validate_profile_name(name)?;
    if name == DEFAULT_PROFILE {
        return Ok(DEFAULT_ROUTER_URL.to_owned());
    }
    config
        .profiles
        .get(name)
        .map(|profile| profile.router_url.clone())
        .ok_or(ConfigError::InvalidProfile)
}

fn expand_home(value: &OsStr) -> Result<PathBuf, ConfigError> {
    let path = PathBuf::from(value);
    if path == Path::new("~") {
        return home_dir().ok_or(ConfigError::Required);
    }
    if let Ok(rest) = path.strip_prefix(Path::new("~/")) {
        return home_dir()
            .map(|home| home.join(rest))
            .ok_or(ConfigError::Required);
    }
    if value == OsStr::new("") {
        Err(ConfigError::Invalid)
    } else {
        Ok(path)
    }
}
