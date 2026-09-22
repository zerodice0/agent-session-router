pub mod install;
pub mod routes;

use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{File, Metadata},
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Component, Path},
    sync::atomic::{AtomicBool, Ordering},
};

use rustix::fs::{Mode, OFlags, open, openat};
use sha2::{Digest, Sha256};

use crate::onboarding::{BootstrapManifest, VERSION, validate_artifacts};

pub const MANIFEST_FILE: &str = "bootstrap-manifest.json";
const MAX_MANIFEST_BYTES: u64 = 128 * 1024;
const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::CLOEXEC)
    .union(OFlags::NOFOLLOW);
const FILE_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::CLOEXEC)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("bootstrap_assets_invalid")]
pub struct BootstrapError;

/// A startup-verified bundle. Original descriptors remain open for its lifetime.
///
/// Serving reopens only whitelisted basenames relative to the retained directory,
/// checking their identity against the originals. Unlike `File::try_clone`, these
/// descriptors have independent offsets. A detected change disables the entire
/// bundle until restart. Clients must still verify downloads: an owner can write
/// an already-open regular file after the last metadata check.
#[derive(Debug)]
pub struct BootstrapAssets {
    manifest: BootstrapManifest,
    manifest_sha256: String,
    directories: Vec<VerifiedDirectory>,
    files: BTreeMap<String, VerifiedFile>,
    invalidated: AtomicBool,
}

#[derive(Debug)]
struct VerifiedDirectory {
    name: OsString,
    file: File,
    metadata: FileMetadata,
}

#[derive(Debug)]
struct VerifiedFile {
    file: File,
    metadata: FileMetadata,
}

/// Access time is deliberately excluded: reading a verified file may update it.
#[derive(Debug, Eq, PartialEq)]
struct FileMetadata {
    device: u64,
    inode: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    links: u64,
    len: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

impl From<&Metadata> for FileMetadata {
    fn from(metadata: &Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            mode: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            links: metadata.nlink(),
            len: metadata.len(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }
}

impl FileMetadata {
    fn same_directory_identity(&self, other: &Self) -> bool {
        self.device == other.device
            && self.inode == other.inode
            && self.mode == other.mode
            && self.uid == other.uid
            && self.gid == other.gid
    }
}

impl BootstrapAssets {
    /// Absence of the configured directory is the only successful `None` case.
    /// A present directory with missing or unsafe contents is an error.
    pub fn load(directory: &Path) -> Result<Option<Self>, BootstrapError> {
        let Some(directories) = open_directories(directory)? else {
            return Ok(None);
        };
        let directory_file = &directories.last().ok_or(BootstrapError)?.file;
        let mut manifest_file = open_asset(directory_file, MANIFEST_FILE)?;
        if manifest_file.metadata.len == 0 || manifest_file.metadata.len > MAX_MANIFEST_BYTES {
            return Err(BootstrapError);
        }
        let mut bytes = Vec::new();
        (&mut manifest_file.file)
            .take(MAX_MANIFEST_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| BootstrapError)?;
        if bytes.len() as u64 != manifest_file.metadata.len {
            return Err(BootstrapError);
        }
        manifest_file.check_unchanged()?;
        let manifest: BootstrapManifest =
            serde_json::from_slice(&bytes).map_err(|_| BootstrapError)?;
        if manifest.version != VERSION || manifest.asr_version.trim().is_empty() {
            return Err(BootstrapError);
        }
        validate_artifacts(&manifest.artifacts).map_err(|_| BootstrapError)?;
        let manifest_sha256 = format!("{:x}", Sha256::digest(&bytes));
        let mut files = BTreeMap::new();
        for artifact in &manifest.artifacts {
            for (name, digest, size) in [
                (
                    &artifact.binary_file,
                    &artifact.binary_sha256,
                    artifact.binary_bytes,
                ),
                (
                    &artifact.archive_file,
                    &artifact.archive_sha256,
                    artifact.archive_bytes,
                ),
            ] {
                let mut asset = open_asset(directory_file, name)?;
                asset.verify_bytes(digest, size)?;
                if files.insert(name.clone(), asset).is_some() {
                    return Err(BootstrapError);
                }
            }
        }
        files.insert(MANIFEST_FILE.to_owned(), manifest_file);
        let assets = Self {
            manifest,
            manifest_sha256,
            directories,
            files,
            invalidated: AtomicBool::new(false),
        };
        assets.check_unchanged()?;
        Ok(Some(assets))
    }

    #[must_use]
    pub fn manifest(&self) -> &BootstrapManifest {
        &self.manifest
    }

    #[must_use]
    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    /// Returns an independently positioned, verified descriptor and exact length.
    /// Unknown names never reach a filesystem operation.
    pub fn file(&self, name: &str) -> Result<Option<(File, u64)>, BootstrapError> {
        if self.invalidated.load(Ordering::Acquire) {
            return Err(BootstrapError);
        }
        let Some((stored_name, verified)) = self.files.get_key_value(name) else {
            return Ok(None);
        };
        let result = (|| {
            self.check_unchanged()?;
            let directory = &self.directories.last().ok_or(BootstrapError)?.file;
            let opened = open_asset(directory, stored_name)?;
            if opened.metadata != verified.metadata {
                return Err(BootstrapError);
            }
            // Recheck after opening to catch replacement during the open itself.
            self.check_unchanged()?;
            if self.invalidated.load(Ordering::Acquire) {
                return Err(BootstrapError);
            }
            Ok(Some((opened.file, opened.metadata.len)))
        })();
        if result.is_err() {
            self.invalidated.store(true, Ordering::Release);
        }
        result
    }

    fn check_unchanged(&self) -> Result<(), BootstrapError> {
        for (index, directory) in self.directories.iter().enumerate() {
            let current = metadata(&directory.file)?;
            if !directory.metadata.same_directory_identity(&current) {
                return Err(BootstrapError);
            }
            if let Some(parent) = index.checked_sub(1).map(|index| &self.directories[index]) {
                let reopened = File::from(
                    openat(
                        &parent.file,
                        &directory.name,
                        DIRECTORY_FLAGS,
                        Mode::empty(),
                    )
                    .map_err(|_| BootstrapError)?,
                );
                if !directory
                    .metadata
                    .same_directory_identity(&metadata(&reopened)?)
                {
                    return Err(BootstrapError);
                }
            }
            // Sibling changes in shared ancestors are harmless; changes inside
            // the actual bundle directory are not.
            if index + 1 == self.directories.len() && current != directory.metadata {
                return Err(BootstrapError);
            }
        }
        for asset in self.files.values() {
            asset.check_unchanged()?;
        }
        Ok(())
    }
}

impl VerifiedFile {
    fn check_unchanged(&self) -> Result<(), BootstrapError> {
        if metadata(&self.file)? != self.metadata {
            return Err(BootstrapError);
        }
        Ok(())
    }

    fn verify_bytes(
        &mut self,
        expected_digest: &str,
        expected_size: u64,
    ) -> Result<(), BootstrapError> {
        if self.metadata.len != expected_size {
            return Err(BootstrapError);
        }
        let mut reader = (&mut self.file).take(expected_size + 1);
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; 8192];
        let mut total = 0_u64;
        loop {
            let count = reader.read(&mut buffer).map_err(|_| BootstrapError)?;
            if count == 0 {
                break;
            }
            total += count as u64;
            digest.update(&buffer[..count]);
        }
        if total != expected_size || format!("{:x}", digest.finalize()) != expected_digest {
            return Err(BootstrapError);
        }
        self.check_unchanged()
    }
}

fn metadata(file: &File) -> Result<FileMetadata, BootstrapError> {
    file.metadata()
        .map(|metadata| FileMetadata::from(&metadata))
        .map_err(|_| BootstrapError)
}

fn open_asset(directory: &File, name: &str) -> Result<VerifiedFile, BootstrapError> {
    let file =
        File::from(openat(directory, name, FILE_FLAGS, Mode::empty()).map_err(|_| BootstrapError)?);
    let metadata = file.metadata().map_err(|_| BootstrapError)?;
    if !metadata.is_file()
        || metadata.uid() != rustix::process::getuid().as_raw()
        || metadata.mode() & 0o7022 != 0
        || metadata.nlink() != 1
    {
        return Err(BootstrapError);
    }
    Ok(VerifiedFile {
        file,
        metadata: FileMetadata::from(&metadata),
    })
}

fn open_directories(path: &Path) -> Result<Option<Vec<VerifiedDirectory>>, BootstrapError> {
    if path.as_os_str().is_empty() {
        return Err(BootstrapError);
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|_| BootstrapError)?
            .join(path)
    };
    if absolute
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(BootstrapError);
    }
    let root = File::from(
        open(Path::new("/"), DIRECTORY_FLAGS, Mode::empty()).map_err(|_| BootstrapError)?,
    );
    let mut directories = vec![VerifiedDirectory {
        name: OsString::from("/"),
        metadata: metadata(&root)?,
        file: root,
    }];
    validate_directory(&directories[0].file, false)?;
    for component in absolute.components() {
        let Component::Normal(name) = component else {
            continue;
        };
        let parent = &directories.last().ok_or(BootstrapError)?.file;
        let file = match openat(parent, name, DIRECTORY_FLAGS, Mode::empty()) {
            Ok(file) => File::from(file),
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(_) => return Err(BootstrapError),
        };
        validate_directory(&file, false)?;
        directories.push(VerifiedDirectory {
            name: name.to_os_string(),
            metadata: metadata(&file)?,
            file,
        });
    }
    validate_directory(&directories.last().ok_or(BootstrapError)?.file, true)?;
    Ok(Some(directories))
}

fn validate_directory(file: &File, bundle: bool) -> Result<(), BootstrapError> {
    let metadata = file.metadata().map_err(|_| BootstrapError)?;
    let owned = metadata.uid() == rustix::process::getuid().as_raw();
    let trusted_ancestor = owned || metadata.uid() == 0;
    let writable = metadata.mode() & 0o022 != 0;
    let sticky = metadata.mode() & 0o1000 != 0;
    if !metadata.is_dir()
        || (bundle && (!owned || metadata.mode() & 0o7022 != 0))
        || (!bundle && (!trusted_ancestor || (writable && !sticky)))
    {
        return Err(BootstrapError);
    }
    Ok(())
}
