use std::{
    fs::{self, DirBuilder, File, Metadata},
    io::{self, Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt},
    path::{Component, Path, PathBuf},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rustix::fs::{Mode, OFlags, open};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::protocol::{AgentClient, AgentSide, RouterErrorCode, WorkspaceName, is_agent_id};

pub const CREDENTIAL_VERSION: u8 = 1;
pub const TOKEN_BYTES: usize = 32;
pub const TOKEN_LENGTH: usize = 43;
pub const MAX_CREDENTIAL_BYTES: usize = 64 * 1024;
pub const MAX_GRANTS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CredentialRole {
    Agent,
    Operator,
}

impl CredentialRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Operator => "operator",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PublicCredentialClaims {
    pub version: u8,
    pub id: Uuid,
    pub role: CredentialRole,
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_side: Option<AgentSide>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_client: Option<AgentClient>,
    pub workspaces: Vec<WorkspaceName>,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CredentialFile {
    pub version: u8,
    pub id: Uuid,
    pub token: SecretToken,
    pub role: CredentialRole,
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_side: Option<AgentSide>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_client: Option<AgentClient>,
    pub workspaces: Vec<WorkspaceName>,
}

impl CredentialFile {
    pub fn generate(
        role: CredentialRole,
        subject: String,
        agent_side: Option<AgentSide>,
        agent_client: Option<AgentClient>,
        workspaces: Vec<WorkspaceName>,
    ) -> Result<Self, CredentialError> {
        let mut bytes = [0_u8; TOKEN_BYTES];
        getrandom::fill(&mut bytes).map_err(|_| CredentialError::Random)?;
        let token = SecretToken(URL_SAFE_NO_PAD.encode(bytes));
        let credential = Self {
            version: CREDENTIAL_VERSION,
            id: Uuid::new_v4(),
            token,
            role,
            subject,
            agent_side,
            agent_client,
            workspaces,
        };
        credential.validate()?;
        Ok(credential)
    }

    pub fn validate(&self) -> Result<(), CredentialError> {
        if self.version != CREDENTIAL_VERSION || self.token.0.len() != TOKEN_LENGTH {
            return Err(CredentialError::Invalid);
        }
        let decoded = URL_SAFE_NO_PAD
            .decode(self.token.0.as_bytes())
            .map_err(|_| CredentialError::Invalid)?;
        if decoded.len() != TOKEN_BYTES
            || URL_SAFE_NO_PAD.encode(&decoded) != self.token.0
            || self.workspaces.len() > MAX_GRANTS
        {
            return Err(CredentialError::Invalid);
        }
        let mut unique = self
            .workspaces
            .iter()
            .map(WorkspaceName::as_str)
            .collect::<Vec<_>>();
        unique.sort_unstable();
        unique.dedup();
        if unique.len() != self.workspaces.len() {
            return Err(CredentialError::Invalid);
        }
        match self.role {
            CredentialRole::Agent => {
                if !is_agent_id(&self.subject)
                    || !valid_agent_claims(self.agent_side, self.agent_client)
                {
                    return Err(CredentialError::Invalid);
                }
            }
            CredentialRole::Operator => {
                if self.subject.is_empty()
                    || self.subject.len() > 119
                    || !self.subject.is_ascii()
                    || !self.subject.chars().all(|value| !value.is_control())
                    || self.subject.starts_with("system:")
                    || self.agent_side.is_some()
                    || self.agent_client.is_some()
                {
                    return Err(CredentialError::Invalid);
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn public_claims(&self) -> PublicCredentialClaims {
        PublicCredentialClaims {
            version: self.version,
            id: self.id,
            role: self.role,
            subject: self.subject.clone(),
            agent_side: self.agent_side,
            agent_client: self.agent_client,
            workspaces: self.workspaces.clone(),
        }
    }

    #[must_use]
    pub fn token_hash(&self) -> [u8; 32] {
        hash_token(self.token.expose())
    }

    #[must_use]
    pub fn token(&self) -> &SecretToken {
        &self.token
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretToken(String);

impl SecretToken {
    pub fn parse(value: String) -> Result<Self, CredentialError> {
        let token = Self(value);
        if token.0.len() != TOKEN_LENGTH
            || URL_SAFE_NO_PAD
                .decode(token.0.as_bytes())
                .map_or(true, |decoded| decoded.len() != TOKEN_BYTES)
        {
            return Err(CredentialError::Invalid);
        }
        Ok(token)
    }

    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Error)]
pub enum CredentialError {
    #[error("credential_invalid")]
    Invalid,
    #[error("credential_permissions")]
    Permissions,
    #[error("credential_io")]
    Io(#[from] io::Error),
    #[error("credential_random")]
    Random,
    #[error("credential_exists")]
    Exists,
}

impl From<CredentialError> for RouterErrorCode {
    fn from(value: CredentialError) -> Self {
        match value {
            CredentialError::Invalid | CredentialError::Permissions => Self::ConfigurationRequired,
            CredentialError::Io(_) | CredentialError::Random | CredentialError::Exists => {
                Self::StorageError
            }
        }
    }
}

#[must_use]
pub fn hash_token(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

pub fn read_credential(path: &Path) -> Result<CredentialFile, CredentialError> {
    let file = open_private_file(path)?;
    let mut bytes = Vec::new();
    file.take((MAX_CREDENTIAL_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_CREDENTIAL_BYTES {
        return Err(CredentialError::Invalid);
    }
    let credential: CredentialFile =
        serde_json::from_slice(&bytes).map_err(|_| CredentialError::Invalid)?;
    credential.validate()?;
    Ok(credential)
}

pub fn write_credential_exclusive(
    path: &Path,
    credential: &CredentialFile,
) -> Result<(), CredentialError> {
    credential.validate()?;
    let parent = path.parent().ok_or(CredentialError::Invalid)?;
    ensure_private_directory(parent, true)?;
    let payload = serde_json::to_vec(credential).map_err(|_| CredentialError::Invalid)?;
    let fd = open(
        path,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|error| {
        let error = io::Error::from_raw_os_error(error.raw_os_error());
        if error.kind() == io::ErrorKind::AlreadyExists {
            CredentialError::Exists
        } else {
            CredentialError::Io(error)
        }
    })?;
    let mut file = File::from(fd);
    validate_private_metadata(&file.metadata()?, false)?;
    file.write_all(&payload)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub fn write_credential_atomic_no_replace(
    directory: &Path,
    file_name: &str,
    credential: &CredentialFile,
) -> Result<PathBuf, CredentialError> {
    ensure_private_directory(directory, true)?;
    if file_name.is_empty() || Path::new(file_name).components().count() != 1 {
        return Err(CredentialError::Invalid);
    }
    let target = directory.join(file_name);
    let temp = directory.join(format!(".credential-{}.tmp", Uuid::new_v4()));
    let result = (|| {
        write_credential_exclusive(&temp, credential)?;
        fs::hard_link(&temp, &target).map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                CredentialError::Exists
            } else {
                CredentialError::Io(error)
            }
        })?;
        File::open(directory)?.sync_all()?;
        Ok(target.clone())
    })();
    let _ = fs::remove_file(&temp);
    result
}

pub fn ensure_private_directory(path: &Path, create: bool) -> Result<(), CredentialError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            validate_private_parents(path)?;
            validate_private_metadata(&metadata, true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound && create => {
            let parent = path.parent().ok_or(CredentialError::Invalid)?;
            validate_existing_ancestors(parent)?;
            let mut builder = DirBuilder::new();
            builder.recursive(true).mode(0o700);
            builder.create(path)?;
            validate_private_parents(path)?;
            validate_private_metadata(&fs::symlink_metadata(path)?, true)
        }
        Err(error) => Err(CredentialError::Io(error)),
    }
}

pub fn open_private_file(path: &Path) -> Result<File, CredentialError> {
    let parent = path.parent().ok_or(CredentialError::Invalid)?;
    validate_private_parents(parent)?;
    validate_private_metadata(&fs::symlink_metadata(parent)?, true)?;
    let fd = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|error| CredentialError::Io(io::Error::from_raw_os_error(error.raw_os_error())))?;
    let file = File::from(fd);
    validate_private_metadata(&file.metadata()?, false)?;
    Ok(file)
}

fn validate_private_parents(path: &Path) -> Result<(), CredentialError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::ParentDir => {
                return Err(CredentialError::Permissions);
            }
            Component::RootDir => current.push(Path::new("/")),
            Component::CurDir => {}
            Component::Normal(value) => {
                current.push(value);
                let metadata = fs::symlink_metadata(&current)?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(CredentialError::Permissions);
                }
                if is_private_tree_component(&current) {
                    validate_private_metadata(&metadata, true)?;
                }
            }
        }
    }
    Ok(())
}

fn validate_existing_ancestors(path: &Path) -> Result<(), CredentialError> {
    let mut current = path;
    while !current.exists() {
        current = current.parent().ok_or(CredentialError::Permissions)?;
    }
    validate_private_parents(current)
}

const fn valid_agent_claims(side: Option<AgentSide>, client: Option<AgentClient>) -> bool {
    matches!(
        (side, client),
        (
            Some(AgentSide::Generic),
            Some(AgentClient::Omp | AgentClient::Generic)
        ) | (
            Some(AgentSide::Claude),
            Some(AgentClient::ClaudeCode | AgentClient::ClaudeSdk)
        ) | (
            Some(AgentSide::Codex),
            Some(AgentClient::CodexCli | AgentClient::CodexAppServer)
        )
    )
}

fn is_private_tree_component(path: &Path) -> bool {
    path.file_name().is_some_and(|name| {
        name == "agent-session-router"
            || name == "credentials"
            || name.to_string_lossy().starts_with("asr-")
            || name.to_string_lossy().starts_with(".asr-")
    })
}

fn validate_private_metadata(metadata: &Metadata, directory: bool) -> Result<(), CredentialError> {
    let expected_kind = if directory {
        metadata.is_dir()
    } else {
        metadata.is_file()
    };
    if !expected_kind
        || metadata.uid() != rustix::process::getuid().as_raw()
        || metadata.mode() & 0o077 != 0
    {
        return Err(CredentialError::Permissions);
    }
    Ok(())
}
