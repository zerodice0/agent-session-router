use std::{
    env,
    ffi::{OsStr, OsString},
    fs::{self, DirBuilder, File, OpenOptions},
    io::{self, Read as _, Write as _},
    os::unix::fs::{
        DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _,
    },
    path::{Component, Path, PathBuf},
};

use rustix::fs::{Mode, OFlags, RenameFlags, open, renameat_with};
use thiserror::Error;
use uuid::Uuid;

pub const INTEGRATIONS_ENV: &str = "ASR_INTEGRATIONS_DIR";
pub const BINARY_NAME: &str = "asr";
pub const LEGACY_LAUNCHER_NAME: &str = "agent-session-router";
const SHARE_RELATIVE: &str = "share/agent-session-router/integrations";
const REQUIRED_ASSETS: [&str; 5] = [
    "omp/index.js",
    "omp/package.json",
    "claude-sdk/bridge.js",
    "claude-sdk/manifest.json",
    "claude-sdk/package.json",
];

#[derive(Debug, Error)]
pub enum AssetError {
    #[error("invalid integration asset path")]
    InvalidPath,
    #[error("integration asset is unavailable")]
    Unavailable,
    #[error("integration asset I/O failed")]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum InstallError {
    #[error("HOME is unavailable; pass --bin-dir explicitly")]
    HomeUnavailable,
    #[error("invalid installation path: {path}")]
    InvalidPath { path: PathBuf },
    #[error("unsafe file type, ownership, or symlink at {path}")]
    UnsafePath { path: PathBuf },
    #[error("installation target already exists with different content: {path}")]
    Conflict { path: PathBuf },
    #[error(
        "legacy source-checkout Python launcher symlink at {path} points to {target}; remove it manually before installing the native asr binary"
    )]
    SourceCheckoutSymlink { path: PathBuf, target: PathBuf },
    #[error("invalid native distribution assets at {path}")]
    InvalidDistribution { path: PathBuf },
    #[error("installation I/O failed")]
    Io(#[from] io::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallOutcome {
    Installed,
    AlreadyInstalled,
}

/// Returns the strict installed integration root for an executable.
///
/// The executable is expected at `<prefix>/bin/asr`; no checkout-relative path is considered.
pub fn installed_integrations_dir(current_executable: &Path) -> Result<PathBuf, AssetError> {
    let bin_dir = current_executable.parent().ok_or(AssetError::InvalidPath)?;
    let prefix = bin_dir.parent().ok_or(AssetError::InvalidPath)?;
    Ok(prefix.join(SHARE_RELATIVE))
}

/// Resolves one runtime integration entrypoint using only the explicit environment override or
/// the installed distribution layout. A configured but invalid override never falls back.
pub fn resolve_integration_asset(
    source_environment: &[(OsString, OsString)],
    current_executable: &Path,
    relative: &Path,
) -> Result<PathBuf, AssetError> {
    if !is_strict_relative(relative) {
        return Err(AssetError::InvalidPath);
    }
    let configured = source_environment
        .iter()
        .find(|(key, _)| key == OsStr::new(INTEGRATIONS_ENV))
        .map(|(_, value)| PathBuf::from(value));
    let root = match configured {
        Some(path) if path.is_absolute() => path,
        Some(_) => return Err(AssetError::InvalidPath),
        None => installed_integrations_dir(current_executable)?,
    };
    let asset = root.join(relative);
    let metadata = fs::symlink_metadata(&asset).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            AssetError::Unavailable
        } else {
            AssetError::Io(error)
        }
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(AssetError::Unavailable);
    }
    Ok(asset)
}

#[must_use]
pub fn default_bin_dir_from(source_environment: &[(OsString, OsString)]) -> Option<PathBuf> {
    source_environment
        .iter()
        .find(|(key, value)| key == OsStr::new("HOME") && !value.is_empty())
        .map(|(_, value)| PathBuf::from(value).join(".local/bin"))
}

pub fn default_bin_dir() -> Result<PathBuf, InstallError> {
    env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(|home| PathBuf::from(home).join(".local/bin"))
        .ok_or(InstallError::HomeUnavailable)
}

/// Installs the running executable. Supplying `None` uses `$HOME/.local/bin`.
pub fn install_current(bin_dir: Option<&Path>) -> Result<InstallOutcome, InstallError> {
    let current_executable = env::current_exe()?;
    let default;
    let destination = if let Some(path) = bin_dir {
        path
    } else {
        default = default_bin_dir()?;
        &default
    };
    install_from(&current_executable, destination)
}

/// Installs a native distribution without replacing any existing path.
///
/// Integration assets, when adjacent to `current_executable`, are completely staged and checked
/// before publication. The integration directory and binary are each published with no-replace
/// renames, but the two publications are intentionally not claimed to be one atomic transaction.
pub fn install_from(
    current_executable: &Path,
    bin_dir: &Path,
) -> Result<InstallOutcome, InstallError> {
    let bin_dir = absolute_path(bin_dir)?;
    let prefix = bin_dir
        .parent()
        .ok_or_else(|| InstallError::InvalidPath {
            path: bin_dir.clone(),
        })?
        .to_path_buf();
    ensure_owned_directory(&prefix, true, 0o755)?;
    let bin_relative = bin_dir
        .strip_prefix(&prefix)
        .map_err(|_| InstallError::InvalidPath {
            path: bin_dir.clone(),
        })?;
    validate_owned_subdirectories(&prefix, bin_relative)?;
    validate_owned_subdirectories(&prefix, Path::new("share/agent-session-router"))?;

    let adjacent_assets = adjacent_assets(current_executable)?;
    let staging = StagingDir::create(&prefix, ".asr-install-")?;
    let staged_assets = if let Some(source) = adjacent_assets.as_deref() {
        let destination = staging.path().join(SHARE_RELATIVE);
        copy_tree(source, &destination)?;
        validate_integration_tree(&destination)?;
        Some(destination)
    } else {
        None
    };
    let staged_binary = staging.path().join("bin").join(BINARY_NAME);
    copy_binary(current_executable, &staged_binary)?;

    let target_binary = bin_dir.join(BINARY_NAME);
    reject_legacy_python_symlinks(&bin_dir, &target_binary)?;
    let target_assets = prefix.join(SHARE_RELATIVE);
    let binary_same = existing_file_matches(&staged_binary, &target_binary)?;
    let assets_same = if let Some(source) = &staged_assets {
        existing_tree_matches(source, &target_assets)?
    } else {
        reject_unexpected_existing_path(&target_assets)?;
        true
    };

    if binary_same && assets_same {
        staging.cleanup()?;
        return Ok(InstallOutcome::AlreadyInstalled);
    }

    if let Some(staged_assets) = staged_assets.as_deref()
        && !assets_same
    {
        create_owned_subdirectories(&prefix, Path::new("share/agent-session-router"), 0o755)?;
        publish_tree_no_replace(staged_assets, &target_assets)?;
    }

    if !binary_same {
        create_owned_subdirectories(&prefix, bin_relative, 0o755)?;
        publish_file_no_replace(&staged_binary, &target_binary)?;
    }

    staging.cleanup()?;
    Ok(InstallOutcome::Installed)
}

/// Creates an archive-ready root with exactly the native distribution path prefixes.
/// The completed root is published with a no-replace rename.
pub fn stage_archive_root(
    source_binary: &Path,
    source_integrations: &Path,
    archive_root: &Path,
) -> Result<(), InstallError> {
    let archive_root = absolute_path(archive_root)?;
    let parent = archive_root
        .parent()
        .ok_or_else(|| InstallError::InvalidPath {
            path: archive_root.clone(),
        })?;
    ensure_owned_directory(parent, true, 0o755)?;
    reject_unexpected_existing_path(&archive_root)?;

    let staging = StagingDir::create(parent, ".asr-archive-")?;
    let staged_binary = staging.path().join("bin").join(BINARY_NAME);
    copy_binary(source_binary, &staged_binary)?;
    let staged_assets = staging.path().join(SHARE_RELATIVE);
    copy_tree(source_integrations, &staged_assets)?;
    validate_integration_tree(&staged_assets)?;
    fs::set_permissions(staging.path(), fs::Permissions::from_mode(0o755))?;
    sync_directory(staging.path())?;

    match rename_no_replace(staging.path(), &archive_root) {
        Ok(()) => {
            sync_directory(parent)?;
            staging.disarm();
            Ok(())
        }
        Err(PublishError::Exists) => Err(InstallError::Conflict { path: archive_root }),
        Err(PublishError::Io(error)) => Err(InstallError::Io(error)),
    }
}

fn is_strict_relative(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn absolute_path(path: &Path) -> Result<PathBuf, InstallError> {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::RootDir => normalized.push(Path::new("/")),
            Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::ParentDir => {
                if !normalized.pop() {
                    return Err(InstallError::InvalidPath { path: joined });
                }
            }
            Component::Prefix(_) => return Err(InstallError::InvalidPath { path: joined }),
        }
    }
    if !normalized.is_absolute() {
        return Err(InstallError::InvalidPath { path: normalized });
    }
    Ok(normalized)
}

fn adjacent_assets(current_executable: &Path) -> Result<Option<PathBuf>, InstallError> {
    let candidate =
        installed_integrations_dir(current_executable).map_err(|_| InstallError::InvalidPath {
            path: current_executable.to_path_buf(),
        })?;
    match fs::symlink_metadata(&candidate) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            validate_owned(&candidate, &metadata)?;
            Ok(Some(candidate))
        }
        Ok(_) => Err(InstallError::InvalidDistribution { path: candidate }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(InstallError::Io(error)),
    }
}

fn reject_legacy_python_symlinks(bin_dir: &Path, target_binary: &Path) -> Result<(), InstallError> {
    reject_python_symlink(target_binary)?;
    reject_python_symlink(&bin_dir.join(LEGACY_LAUNCHER_NAME))
}

fn reject_python_symlink(path: &Path) -> Result<(), InstallError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(InstallError::Io(error)),
    };
    if !metadata.file_type().is_symlink() {
        return Ok(());
    }
    let target = fs::read_link(path)?;
    if target.extension() == Some(OsStr::new("py"))
        || target.file_name() == Some(OsStr::new("asr.py"))
    {
        return Err(InstallError::SourceCheckoutSymlink {
            path: path.to_path_buf(),
            target,
        });
    }
    Err(InstallError::UnsafePath {
        path: path.to_path_buf(),
    })
}

fn validate_integration_tree(path: &Path) -> Result<(), InstallError> {
    for relative in REQUIRED_ASSETS {
        let required = path.join(relative);
        let metadata = fs::symlink_metadata(&required).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                InstallError::InvalidDistribution {
                    path: required.clone(),
                }
            } else {
                InstallError::Io(error)
            }
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(InstallError::InvalidDistribution { path: required });
        }
    }
    Ok(())
}

fn copy_binary(source: &Path, destination: &Path) -> Result<(), InstallError> {
    let parent = destination
        .parent()
        .ok_or_else(|| InstallError::InvalidPath {
            path: destination.to_path_buf(),
        })?;
    create_private_directory_tree(parent)?;
    copy_regular_file(source, destination, Some(0o755))
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), InstallError> {
    let metadata = fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(InstallError::UnsafePath {
            path: source.to_path_buf(),
        });
    }
    validate_owned(source, &metadata)?;
    let parent = destination
        .parent()
        .ok_or_else(|| InstallError::InvalidPath {
            path: destination.to_path_buf(),
        })?;
    ensure_owned_directory(parent, true, 0o755)?;
    create_directory(destination, 0o755)?;

    let mut entries = fs::read_dir(source)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&source_path)?;
        if metadata.file_type().is_symlink() {
            return Err(InstallError::UnsafePath { path: source_path });
        }
        if metadata.is_dir() {
            copy_tree(&source_path, &destination_path)?;
        } else if metadata.is_file() {
            copy_regular_file(&source_path, &destination_path, None)?;
        } else {
            return Err(InstallError::UnsafePath { path: source_path });
        }
    }
    sync_directory(destination)
}

fn copy_regular_file(
    source: &Path,
    destination: &Path,
    forced_mode: Option<u32>,
) -> Result<(), InstallError> {
    let mut input = open_regular_no_follow(source)?;
    let source_metadata = input.metadata()?;
    let mode = forced_mode.unwrap_or_else(|| sanitized_asset_mode(source_metadata.mode()));
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(destination)?;
    io::copy(&mut input, &mut output)?;
    output.set_permissions(fs::Permissions::from_mode(mode))?;
    output.flush()?;
    output.sync_all()?;
    Ok(())
}

const fn sanitized_asset_mode(source_mode: u32) -> u32 {
    if source_mode & 0o111 == 0 {
        0o644
    } else {
        0o755
    }
}

fn open_regular_no_follow(path: &Path) -> Result<File, InstallError> {
    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|error| io::Error::from_raw_os_error(error.raw_os_error()))?;
    let file = File::from(descriptor);
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(InstallError::UnsafePath {
            path: path.to_path_buf(),
        });
    }
    validate_owned(path, &metadata)?;
    Ok(file)
}

fn existing_file_matches(expected: &Path, target: &Path) -> Result<bool, InstallError> {
    let metadata = match fs::symlink_metadata(target) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(InstallError::Io(error)),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(InstallError::UnsafePath {
            path: target.to_path_buf(),
        });
    }
    validate_owned(target, &metadata)?;
    if metadata.mode() & 0o777 != 0o755 || !same_file_bytes(expected, target)? {
        return Err(InstallError::Conflict {
            path: target.to_path_buf(),
        });
    }
    Ok(true)
}

fn existing_tree_matches(expected: &Path, target: &Path) -> Result<bool, InstallError> {
    let target_metadata = match fs::symlink_metadata(target) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(InstallError::Io(error)),
    };
    if target_metadata.file_type().is_symlink() || !target_metadata.is_dir() {
        return Err(InstallError::UnsafePath {
            path: target.to_path_buf(),
        });
    }
    compare_trees(expected, target)?;
    Ok(true)
}

fn compare_trees(expected: &Path, target: &Path) -> Result<(), InstallError> {
    let expected_metadata = fs::symlink_metadata(expected)?;
    let target_metadata = fs::symlink_metadata(target)?;
    if expected_metadata.file_type().is_symlink()
        || target_metadata.file_type().is_symlink()
        || !expected_metadata.is_dir()
        || !target_metadata.is_dir()
    {
        return Err(InstallError::UnsafePath {
            path: target.to_path_buf(),
        });
    }
    validate_owned(target, &target_metadata)?;
    if expected_metadata.mode() & 0o777 != target_metadata.mode() & 0o777 {
        return Err(InstallError::Conflict {
            path: target.to_path_buf(),
        });
    }

    let mut expected_entries = fs::read_dir(expected)?.collect::<Result<Vec<_>, _>>()?;
    let mut target_entries = fs::read_dir(target)?.collect::<Result<Vec<_>, _>>()?;
    expected_entries.sort_by_key(fs::DirEntry::file_name);
    target_entries.sort_by_key(fs::DirEntry::file_name);
    let expected_names = expected_entries
        .iter()
        .map(fs::DirEntry::file_name)
        .collect::<Vec<_>>();
    let target_names = target_entries
        .iter()
        .map(fs::DirEntry::file_name)
        .collect::<Vec<_>>();
    if expected_names != target_names {
        return Err(InstallError::Conflict {
            path: target.to_path_buf(),
        });
    }

    for (expected_entry, target_entry) in expected_entries.iter().zip(target_entries.iter()) {
        let expected_path = expected_entry.path();
        let target_path = target_entry.path();
        let expected_metadata = fs::symlink_metadata(&expected_path)?;
        let target_metadata = fs::symlink_metadata(&target_path)?;
        if expected_metadata.file_type().is_symlink() || target_metadata.file_type().is_symlink() {
            return Err(InstallError::UnsafePath { path: target_path });
        }
        if expected_metadata.is_dir() && target_metadata.is_dir() {
            compare_trees(&expected_path, &target_path)?;
        } else if expected_metadata.is_file() && target_metadata.is_file() {
            validate_owned(&target_path, &target_metadata)?;
            if expected_metadata.mode() & 0o777 != target_metadata.mode() & 0o777
                || !same_file_bytes(&expected_path, &target_path)?
            {
                return Err(InstallError::Conflict { path: target_path });
            }
        } else {
            return Err(InstallError::Conflict { path: target_path });
        }
    }
    Ok(())
}

fn same_file_bytes(left: &Path, right: &Path) -> Result<bool, InstallError> {
    let mut left = open_regular_no_follow(left)?;
    let mut right = open_regular_no_follow(right)?;
    if left.metadata()?.len() != right.metadata()?.len() {
        return Ok(false);
    }
    let mut left_buffer = [0_u8; 16 * 1024];
    let mut right_buffer = [0_u8; 16 * 1024];
    loop {
        let left_count = left.read(&mut left_buffer)?;
        let right_count = right.read(&mut right_buffer)?;
        if left_count != right_count || left_buffer[..left_count] != right_buffer[..right_count] {
            return Ok(false);
        }
        if left_count == 0 {
            return Ok(true);
        }
    }
}

fn publish_tree_no_replace(source: &Path, target: &Path) -> Result<(), InstallError> {
    match rename_no_replace(source, target) {
        Ok(()) => sync_directory(target.parent().ok_or_else(|| InstallError::InvalidPath {
            path: target.to_path_buf(),
        })?),
        Err(PublishError::Exists) => {
            if existing_tree_matches(source, target)? {
                Ok(())
            } else {
                Err(InstallError::Conflict {
                    path: target.to_path_buf(),
                })
            }
        }
        Err(PublishError::Io(error)) => Err(InstallError::Io(error)),
    }
}

fn publish_file_no_replace(source: &Path, target: &Path) -> Result<(), InstallError> {
    match rename_no_replace(source, target) {
        Ok(()) => sync_directory(target.parent().ok_or_else(|| InstallError::InvalidPath {
            path: target.to_path_buf(),
        })?),
        Err(PublishError::Exists) => {
            if existing_file_matches(source, target)? {
                Ok(())
            } else {
                Err(InstallError::Conflict {
                    path: target.to_path_buf(),
                })
            }
        }
        Err(PublishError::Io(error)) => Err(InstallError::Io(error)),
    }
}

fn reject_unexpected_existing_path(path: &Path) -> Result<(), InstallError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(InstallError::Io(error)),
        Ok(metadata) if metadata.file_type().is_symlink() => Err(InstallError::UnsafePath {
            path: path.to_path_buf(),
        }),
        Ok(_) => Err(InstallError::Conflict {
            path: path.to_path_buf(),
        }),
    }
}

enum PublishError {
    Exists,
    Io(io::Error),
}

fn rename_no_replace(source: &Path, target: &Path) -> Result<(), PublishError> {
    let source_parent = source.parent().ok_or_else(|| {
        PublishError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "source has no parent",
        ))
    })?;
    let target_parent = target.parent().ok_or_else(|| {
        PublishError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "target has no parent",
        ))
    })?;
    let source_name = source.file_name().ok_or_else(|| {
        PublishError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "source has no file name",
        ))
    })?;
    let target_name = target.file_name().ok_or_else(|| {
        PublishError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "target has no file name",
        ))
    })?;
    let source_directory = open_directory_no_follow(source_parent).map_err(PublishError::Io)?;
    let target_directory = open_directory_no_follow(target_parent).map_err(PublishError::Io)?;
    match renameat_with(
        &source_directory,
        source_name,
        &target_directory,
        target_name,
        RenameFlags::NOREPLACE,
    ) {
        Ok(()) => Ok(()),
        Err(error) if error == rustix::io::Errno::EXIST || error == rustix::io::Errno::NOTEMPTY => {
            Err(PublishError::Exists)
        }
        Err(error) => Err(PublishError::Io(io::Error::from_raw_os_error(
            error.raw_os_error(),
        ))),
    }
}

fn open_directory_no_follow(path: &Path) -> io::Result<File> {
    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|error| io::Error::from_raw_os_error(error.raw_os_error()))?;
    Ok(File::from(descriptor))
}

fn validate_owned_subdirectories(anchor: &Path, relative: &Path) -> Result<(), InstallError> {
    walk_owned_subdirectories(anchor, relative, false, 0o755)
}

fn create_owned_subdirectories(
    anchor: &Path,
    relative: &Path,
    mode: u32,
) -> Result<(), InstallError> {
    walk_owned_subdirectories(anchor, relative, true, mode)
}

fn walk_owned_subdirectories(
    anchor: &Path,
    relative: &Path,
    create: bool,
    mode: u32,
) -> Result<(), InstallError> {
    if !is_strict_relative(relative) {
        return Err(InstallError::InvalidPath {
            path: anchor.join(relative),
        });
    }
    let mut current = anchor.to_path_buf();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(InstallError::InvalidPath {
                path: anchor.join(relative),
            });
        };
        current.push(name);
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound && !create => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut builder = DirBuilder::new();
                builder.mode(mode);
                match builder.create(&current) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(InstallError::Io(error)),
                }
                fs::symlink_metadata(&current)?
            }
            Err(error) => return Err(InstallError::Io(error)),
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(InstallError::UnsafePath {
                path: current.clone(),
            });
        }
        validate_owned(&current, &metadata)?;
    }
    Ok(())
}

fn ensure_owned_directory(path: &Path, create: bool, mode: u32) -> Result<(), InstallError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(InstallError::UnsafePath {
                    path: path.to_path_buf(),
                });
            }
            validate_owned(path, &metadata)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound && create => {
            let mut builder = DirBuilder::new();
            builder.recursive(true).mode(mode);
            builder.create(path)?;
            let metadata = fs::symlink_metadata(path)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(InstallError::UnsafePath {
                    path: path.to_path_buf(),
                });
            }
            validate_owned(path, &metadata)
        }
        Err(error) => Err(InstallError::Io(error)),
    }
}

fn create_private_directory_tree(path: &Path) -> Result<(), InstallError> {
    if path.exists() {
        return ensure_owned_directory(path, false, 0o700);
    }
    let mut builder = DirBuilder::new();
    builder.recursive(true).mode(0o700);
    builder.create(path)?;
    ensure_owned_directory(path, false, 0o700)
}

fn create_directory(path: &Path, mode: u32) -> Result<(), InstallError> {
    let mut builder = DirBuilder::new();
    builder.mode(mode);
    builder.create(path)?;
    let metadata = fs::symlink_metadata(path)?;
    validate_owned(path, &metadata)
}

fn validate_owned(path: &Path, metadata: &fs::Metadata) -> Result<(), InstallError> {
    if metadata.uid() != rustix::process::getuid().as_raw() {
        return Err(InstallError::UnsafePath {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), InstallError> {
    open_directory_no_follow(path)?.sync_all()?;
    Ok(())
}

struct StagingDir {
    path: PathBuf,
    active: bool,
}

impl StagingDir {
    fn create(parent: &Path, prefix: &str) -> Result<Self, InstallError> {
        for _ in 0..8 {
            let path = parent.join(format!("{prefix}{}", Uuid::new_v4()));
            let mut builder = DirBuilder::new();
            builder.mode(0o700);
            match builder.create(&path) {
                Ok(()) => return Ok(Self { path, active: true }),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(InstallError::Io(error)),
            }
        }
        Err(InstallError::Io(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a unique staging directory",
        )))
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn cleanup(mut self) -> Result<(), InstallError> {
        match fs::remove_dir_all(&self.path) {
            Ok(()) => {
                self.active = false;
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.active = false;
                Ok(())
            }
            Err(error) => Err(InstallError::Io(error)),
        }
    }

    fn disarm(mut self) {
        self.active = false;
    }
}

impl Drop for StagingDir {
    fn drop(&mut self) {
        if self.active {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
