use std::{
    collections::BTreeMap,
    env,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

use crate::protocol::{RouterErrorCode, WorkspaceName};

pub const DEFAULT_ROUTER_URL: &str = "ws://127.0.0.1:8787/ws";
pub const DEFAULT_PROFILE: &str = "local";
pub const CONFIG_VERSION: u8 = 1;
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
#[serde(rename_all = "camelCase")]
pub struct Profile {
    pub router_url: String,
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
            | ConfigError::InvalidDelegateContext => Self::InvalidMessage,
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
    match fs::read(path) {
        Ok(bytes) => {
            if bytes.len() > 256 * 1024 {
                return Err(ConfigError::Invalid);
            }
            let config: ConfigFile =
                serde_json::from_slice(&bytes).map_err(|_| ConfigError::Invalid)?;
            validate_config(&config)?;
            Ok(config)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(ConfigFile {
            version: CONFIG_VERSION,
            ..ConfigFile::default()
        }),
        Err(error) => Err(ConfigError::Io(error)),
    }
}

pub fn save_config(path: &Path, config: &ConfigFile) -> Result<(), ConfigError> {
    validate_config(config)?;
    let parent = path.parent().ok_or(ConfigError::Invalid)?;
    fs::create_dir_all(parent)?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    let payload = serde_json::to_vec(config).map_err(|_| ConfigError::Invalid)?;
    let temp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        file.write_all(&payload)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        File::open(parent)?.sync_all()?;
        Ok::<(), io::Error>(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result.map_err(ConfigError::Io)
}

pub fn select(
    explicit_profile: Option<&str>,
    explicit_credential: Option<&Path>,
) -> Result<Selection, ConfigError> {
    let path = config_path()?;
    let config = load_config(&path)?;
    let environment_url = env::var("ROUTER_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let (profile, raw_url) = if let Some(profile) = explicit_profile {
        let raw = profile_url(&config, profile)?;
        (Some(profile.to_owned()), raw)
    } else if let Some(raw) = environment_url {
        (None, raw)
    } else if let Some(profile) = &config.default_profile {
        let raw = profile_url(&config, profile)?;
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
    if config.version != CONFIG_VERSION {
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
