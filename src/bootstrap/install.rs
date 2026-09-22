use std::{
    collections::BTreeSet,
    env, fmt,
    fs::{self, DirBuilder, File, OpenOptions},
    io::{self, Read},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Component, Path, PathBuf},
    time::Duration,
};

use flate2::read::MultiGzDecoder;
use rustix::fs::{Mode, OFlags, mkdirat, open, openat};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::{
    bootstrap::{
        MANIFEST_FILE,
        routes::{VerifiedRoute, onboarding_url},
    },
    install::{InstallError, install_from, installed_integrations_dir},
    onboarding::{
        BootstrapArtifact, BootstrapManifest, OnboardingTicket, VERSION, validate_artifacts,
    },
    protocol::PROTOCOL_VERSION,
};

const MAX_MANIFEST_BYTES: usize = 128 * 1024;
const MAX_ARCHIVE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_EXPANDED_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_ENTRIES: usize = 100_000;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);
const INTEGRATIONS: &str = "share/agent-session-router/integrations";
const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

#[derive(Debug)]
pub struct InstalledBundle {
    pub executable: PathBuf,
    pub integrations_dir: PathBuf,
}

/// Errors never retain server responses, filesystem paths, or invitation material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BundleInstallError {
    InvalidTicket,
    UnsupportedTarget,
    ServerMismatch,
    ManifestMismatch,
    DownloadFailed,
    Redirect,
    ArchiveMismatch,
    InvalidArchive,
    ArchiveLimit,
    BinaryMismatch,
    InvalidDistribution,
    HomeUnavailable,
    UnsafePath,
    Conflict,
    Io,
}

impl BundleInstallError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidTicket => "invalid_ticket",
            Self::UnsupportedTarget => "unsupported_target",
            Self::ServerMismatch => "bootstrap_server_mismatch",
            Self::ManifestMismatch => "bootstrap_manifest_mismatch",
            Self::DownloadFailed => "bootstrap_download_failed",
            Self::Redirect => "bootstrap_redirect_rejected",
            Self::ArchiveMismatch => "bootstrap_archive_mismatch",
            Self::InvalidArchive => "bootstrap_archive_invalid",
            Self::ArchiveLimit => "bootstrap_archive_limit",
            Self::BinaryMismatch => "bootstrap_binary_mismatch",
            Self::InvalidDistribution => "bootstrap_distribution_invalid",
            Self::HomeUnavailable => "home_unavailable",
            Self::UnsafePath => "bootstrap_unsafe_path",
            Self::Conflict => "bootstrap_install_conflict",
            Self::Io => "bootstrap_install_io",
        }
    }
}

impl fmt::Display for BundleInstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for BundleInstallError {}

pub fn host_target() -> Result<&'static str, BundleInstallError> {
    match (env::consts::OS, env::consts::ARCH) {
        ("macos", "aarch64") => Ok("aarch64-apple-darwin"),
        ("macos", "x86_64") => Ok("x86_64-apple-darwin"),
        ("linux", "aarch64") => Ok("aarch64-unknown-linux-gnu"),
        ("linux", "x86_64") => Ok("x86_64-unknown-linux-gnu"),
        _ => Err(BundleInstallError::UnsupportedTarget),
    }
}

/// Installs only distribution bytes. Does not execute them or consume the invite,
/// write a profile, generate credentials, or configure a provider.
pub async fn install_bundle(
    ticket: &OnboardingTicket,
    route: &VerifiedRoute,
) -> Result<InstalledBundle, BundleInstallError> {
    ticket
        .validate()
        .map_err(|_| BundleInstallError::InvalidTicket)?;
    let target = host_target()?;
    let artifact = ticket
        .artifacts
        .iter()
        .find(|artifact| artifact.target == target)
        .ok_or(BundleInstallError::UnsupportedTarget)?;
    if !ticket.routes.contains(&route.route)
        || route.info.version != VERSION
        || route.info.protocol_version != PROTOCOL_VERSION
        || route.info.server_id != ticket.server_id
    {
        return Err(BundleInstallError::ServerMismatch);
    }
    if route.info.manifest_sha256.as_deref() != Some(&ticket.manifest_sha256)
        || !route
            .info
            .available_targets
            .iter()
            .any(|available| available == target)
    {
        return Err(BundleInstallError::ManifestMismatch);
    }
    let manifest = download_manifest(route).await?;
    if format!("{:x}", Sha256::digest(&manifest)) != ticket.manifest_sha256 {
        return Err(BundleInstallError::ManifestMismatch);
    }
    let manifest: BootstrapManifest =
        serde_json::from_slice(&manifest).map_err(|_| BundleInstallError::ManifestMismatch)?;
    if manifest.version != VERSION
        || manifest.asr_version.trim().is_empty()
        || validate_artifacts(&manifest.artifacts).is_err()
        || manifest.artifacts.len() != ticket.artifacts.len()
        || !ticket
            .artifacts
            .iter()
            .all(|entry| manifest.artifacts.contains(entry))
    {
        return Err(BundleInstallError::ManifestMismatch);
    }

    let home = home_directory()?;
    let staging = StagingDirectory::create(&home)?;
    let archive_path = staging.0.join("archive.tar.gz");
    download_archive(route, artifact, &archive_path).await?;
    let artifact = artifact.clone();
    let manifest_sha256 = ticket.manifest_sha256.clone();
    // Extraction and the existing synchronous no-replace installer never block
    // the async route/client executor. The staging guard survives cancellation.
    tokio::task::spawn_blocking(move || {
        let root = staging.0.join("root");
        private_directory(&root)?;
        extract_archive(&archive_path, &root)?;
        let binary = root.join("bin/asr");
        verify_binary(&binary, &artifact)?;
        let assets = root.join(INTEGRATIONS);
        if !fs::symlink_metadata(&assets).is_ok_and(|metadata| metadata.is_dir()) {
            return Err(BundleInstallError::InvalidDistribution);
        }
        // install_from validates the existing REQUIRED_ASSETS contract before
        // publication, avoiding a second asset list that can drift over time.
        let relative = PathBuf::from(".local/share/agent-session-router/versions")
            .join(manifest_sha256)
            .join("bin");
        create_install_directories(&home, &relative)?;
        let bin_dir = home.join(relative);
        install_from(&binary, &bin_dir).map_err(|error| install_error(&error))?;
        let executable = bin_dir.join("asr");
        let integrations_dir = installed_integrations_dir(&executable)
            .map_err(|_| BundleInstallError::InvalidDistribution)?;
        Ok(InstalledBundle {
            executable,
            integrations_dir,
        })
    })
    .await
    .map_err(|_| BundleInstallError::Io)?
}

async fn download_response(
    route: &VerifiedRoute,
    name: &str,
) -> Result<reqwest::Response, BundleInstallError> {
    let url = onboarding_url(
        &route.route.router_url,
        &format!("/onboarding/files/{name}"),
    )
    .map_err(|_| BundleInstallError::InvalidTicket)?;
    let response = route
        .client
        .get(url)
        .timeout(DOWNLOAD_TIMEOUT)
        .send()
        .await
        .map_err(|_| BundleInstallError::DownloadFailed)?;
    if response.status().is_redirection() {
        return Err(BundleInstallError::Redirect);
    }
    if response.status() != reqwest::StatusCode::OK {
        return Err(BundleInstallError::DownloadFailed);
    }
    Ok(response)
}

async fn download_manifest(route: &VerifiedRoute) -> Result<Vec<u8>, BundleInstallError> {
    let mut response = download_response(route, MANIFEST_FILE).await?;
    if response
        .content_length()
        .is_some_and(|size| size > MAX_MANIFEST_BYTES as u64)
    {
        return Err(BundleInstallError::ManifestMismatch);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| BundleInstallError::DownloadFailed)?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_MANIFEST_BYTES {
            return Err(BundleInstallError::ManifestMismatch);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn download_archive(
    route: &VerifiedRoute,
    artifact: &BootstrapArtifact,
    path: &Path,
) -> Result<(), BundleInstallError> {
    if artifact.archive_bytes > MAX_ARCHIVE_BYTES {
        return Err(BundleInstallError::ArchiveLimit);
    }
    let mut response = download_response(route, &artifact.archive_file).await?;
    if response
        .content_length()
        .is_some_and(|size| size != artifact.archive_bytes)
    {
        return Err(BundleInstallError::ArchiveMismatch);
    }
    let mut file = tokio::fs::File::from_std(new_file(path, 0o600)?);
    let mut digest = Sha256::new();
    let mut size = 0_u64;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| BundleInstallError::DownloadFailed)?
    {
        size = size
            .checked_add(chunk.len() as u64)
            .ok_or(BundleInstallError::ArchiveLimit)?;
        if size > artifact.archive_bytes || size > MAX_ARCHIVE_BYTES {
            return Err(BundleInstallError::ArchiveMismatch);
        }
        digest.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|_| BundleInstallError::Io)?;
    }
    if size != artifact.archive_bytes
        || format!("{:x}", digest.finalize()) != artifact.archive_sha256
    {
        return Err(BundleInstallError::ArchiveMismatch);
    }
    file.flush().await.map_err(|_| BundleInstallError::Io)?;
    Ok(())
}

/// Bound physical metadata before tar's logical iterator allocates GNU/PAX
/// extension payloads. The full decompressed stream, not just file contents,
/// counts toward the expansion limit.
fn validate_archive_envelope(path: &Path) -> Result<(), BundleInstallError> {
    let file = File::open(path).map_err(|_| BundleInstallError::Io)?;
    let decoder = MultiGzDecoder::new(file).take(MAX_EXPANDED_BYTES + 1);
    let mut archive = tar::Archive::new(decoder);
    let entries = archive
        .entries()
        .map_err(|_| BundleInstallError::InvalidArchive)?
        .raw(true);
    for (index, entry) in entries.enumerate() {
        if index >= MAX_ENTRIES * 3 {
            return Err(BundleInstallError::ArchiveLimit);
        }
        let entry = entry.map_err(|_| BundleInstallError::InvalidArchive)?;
        let kind = entry.header().entry_type();
        let metadata = kind.is_gnu_longname()
            || kind.is_gnu_longlink()
            || kind.is_pax_local_extensions()
            || kind.is_pax_global_extensions();
        if metadata {
            if entry.size() > 64 * 1024 {
                return Err(BundleInstallError::ArchiveLimit);
            }
        } else if !kind.is_file() && !kind.is_dir() {
            return Err(BundleInstallError::InvalidArchive);
        }
        if entry.size() > MAX_EXPANDED_BYTES {
            return Err(BundleInstallError::ArchiveLimit);
        }
    }
    let mut decoder = archive.into_inner();
    io::copy(&mut decoder, &mut io::sink()).map_err(|_| BundleInstallError::InvalidArchive)?;
    if decoder.limit() == 0 {
        return Err(BundleInstallError::ArchiveLimit);
    }
    Ok(())
}

fn extract_archive(path: &Path, root: &Path) -> Result<(), BundleInstallError> {
    validate_archive_envelope(path)?;
    let file = File::open(path).map_err(|_| BundleInstallError::Io)?;
    let decoder = MultiGzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);
    let mut paths = BTreeSet::new();
    let mut expanded = 0_u64;
    for entry in archive
        .entries()
        .map_err(|_| BundleInstallError::InvalidArchive)?
    {
        let mut entry = entry.map_err(|_| BundleInstallError::InvalidArchive)?;
        if paths.len() >= MAX_ENTRIES {
            return Err(BundleInstallError::ArchiveLimit);
        }
        let kind = entry.header().entry_type();
        let directory = kind.is_dir();
        let source_mode = entry
            .header()
            .mode()
            .map_err(|_| BundleInstallError::InvalidArchive)?;
        if (!kind.is_file() && !directory) || source_mode & 0o6000 != 0 {
            return Err(BundleInstallError::InvalidArchive);
        }
        let relative = entry
            .path()
            .map_err(|_| BundleInstallError::InvalidArchive)?
            .into_owned();
        if relative.as_os_str().is_empty()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
            || !allowed_path(&relative, directory)
            || !paths.insert(relative.clone())
        {
            return Err(BundleInstallError::InvalidArchive);
        }
        let size = entry.size();
        if directory && size != 0 {
            return Err(BundleInstallError::InvalidArchive);
        }
        expanded = expanded
            .checked_add(size)
            .ok_or(BundleInstallError::ArchiveLimit)?;
        if expanded > MAX_EXPANDED_BYTES {
            return Err(BundleInstallError::ArchiveLimit);
        }
        let destination = root.join(&relative);
        if directory {
            private_directory_tree(&destination)?;
        } else {
            private_directory_tree(
                destination
                    .parent()
                    .ok_or(BundleInstallError::InvalidArchive)?,
            )?;
            let mode = if relative == Path::new("bin/asr") || source_mode & 0o111 != 0 {
                0o700
            } else {
                0o600
            };
            let mut output = new_file(&destination, mode)?;
            let copied = io::copy(&mut entry, &mut output)
                .map_err(|_| BundleInstallError::InvalidArchive)?;
            if copied != size {
                return Err(BundleInstallError::InvalidArchive);
            }
        }
    }
    // Force gzip checksum/truncation validation, reject hidden concatenated tar
    // members, and bound optional zero padding after tar's end marker.
    let mut decoder = archive.into_inner();
    let mut padding = 0_usize;
    let mut buffer = [0_u8; 8192];
    loop {
        let count = decoder
            .read(&mut buffer)
            .map_err(|_| BundleInstallError::InvalidArchive)?;
        if count == 0 {
            break;
        }
        padding += count;
        if padding > 1024 * 1024 || buffer[..count].iter().any(|byte| *byte != 0) {
            return Err(BundleInstallError::InvalidArchive);
        }
    }
    Ok(())
}

fn allowed_path(path: &Path, directory: bool) -> bool {
    let allowed_entry = if directory {
        [
            Path::new("bin"),
            Path::new("share"),
            Path::new("share/agent-session-router"),
            Path::new(INTEGRATIONS),
        ]
        .contains(&path)
    } else {
        path == Path::new("bin/asr")
    };
    allowed_entry
        || path
            .strip_prefix(INTEGRATIONS)
            .is_ok_and(|suffix| !suffix.as_os_str().is_empty())
}

fn verify_binary(path: &Path, artifact: &BootstrapArtifact) -> Result<(), BundleInstallError> {
    let mut file = File::open(path).map_err(|_| BundleInstallError::BinaryMismatch)?;
    let metadata = file
        .metadata()
        .map_err(|_| BundleInstallError::BinaryMismatch)?;
    if !metadata.is_file() || metadata.len() != artifact.binary_bytes {
        return Err(BundleInstallError::BinaryMismatch);
    }
    let mut hash = Sha256::new();
    io::copy(&mut file, &mut hash).map_err(|_| BundleInstallError::BinaryMismatch)?;
    if format!("{:x}", hash.finalize()) != artifact.binary_sha256 {
        return Err(BundleInstallError::BinaryMismatch);
    }
    Ok(())
}

fn home_directory() -> Result<PathBuf, BundleInstallError> {
    let home = env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or(BundleInstallError::HomeUnavailable)?;
    if !home.is_absolute()
        || home
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
    {
        return Err(BundleInstallError::UnsafePath);
    }
    // Resolve no symlinks, including HOME's ancestors. Openat keeps each lookup
    // anchored to the directory just checked rather than a re-resolved pathname.
    let mut directory = File::from(
        open("/", DIRECTORY_FLAGS, Mode::empty()).map_err(|_| BundleInstallError::UnsafePath)?,
    );
    for part in home.components() {
        if let Component::Normal(name) = part {
            directory = File::from(
                openat(&directory, name, DIRECTORY_FLAGS, Mode::empty())
                    .map_err(|_| BundleInstallError::UnsafePath)?,
            );
            let metadata = directory.metadata().map_err(|_| BundleInstallError::Io)?;
            let owner = metadata.uid();
            if (owner != 0 && owner != rustix::process::getuid().as_raw())
                || (metadata.mode() & 0o022 != 0 && !(owner == 0 && metadata.mode() & 0o1000 != 0))
            {
                return Err(BundleInstallError::UnsafePath);
            }
        }
    }
    check_owned_directory(&directory)?;
    Ok(home)
}

fn create_install_directories(home: &Path, relative: &Path) -> Result<(), BundleInstallError> {
    let mut directory = File::from(
        open(home, DIRECTORY_FLAGS, Mode::empty()).map_err(|_| BundleInstallError::UnsafePath)?,
    );
    check_owned_directory(&directory)?;
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(BundleInstallError::UnsafePath);
        };
        match mkdirat(&directory, name, Mode::from_raw_mode(0o700)) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
            Err(_) => return Err(BundleInstallError::Io),
        }
        directory = File::from(
            openat(&directory, name, DIRECTORY_FLAGS, Mode::empty())
                .map_err(|_| BundleInstallError::UnsafePath)?,
        );
        check_owned_directory(&directory)?;
    }
    Ok(())
}

fn check_owned_directory(directory: &File) -> Result<(), BundleInstallError> {
    let metadata = directory.metadata().map_err(|_| BundleInstallError::Io)?;
    if metadata.uid() != rustix::process::getuid().as_raw() || metadata.mode() & 0o022 != 0 {
        return Err(BundleInstallError::UnsafePath);
    }
    Ok(())
}

fn new_file(path: &Path, mode: u32) -> Result<File, BundleInstallError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(path)
        .map_err(|_| BundleInstallError::InvalidArchive)
}

fn private_directory(path: &Path) -> Result<(), BundleInstallError> {
    DirBuilder::new()
        .mode(0o700)
        .create(path)
        .map_err(|_| BundleInstallError::Io)
}

fn private_directory_tree(path: &Path) -> Result<(), BundleInstallError> {
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
        .map_err(|_| BundleInstallError::InvalidArchive)
}

fn install_error(error: &InstallError) -> BundleInstallError {
    match error {
        InstallError::Conflict { .. } => BundleInstallError::Conflict,
        InstallError::InvalidDistribution { .. } => BundleInstallError::InvalidDistribution,
        InstallError::UnsafePath { .. }
        | InstallError::InvalidPath { .. }
        | InstallError::SourceCheckoutSymlink { .. } => BundleInstallError::UnsafePath,
        InstallError::HomeUnavailable => BundleInstallError::HomeUnavailable,
        InstallError::Io(_) => BundleInstallError::Io,
    }
}

struct StagingDirectory(PathBuf);
impl StagingDirectory {
    fn create(parent: &Path) -> Result<Self, BundleInstallError> {
        for _ in 0..8 {
            let path = parent.join(format!(".asr-onboarding-{}", Uuid::new_v4()));
            match DirBuilder::new().mode(0o700).create(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(_) => return Err(BundleInstallError::Io),
            }
        }
        Err(BundleInstallError::Io)
    }
}
impl Drop for StagingDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
