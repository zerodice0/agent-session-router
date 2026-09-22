use std::{
    ffi::OsString,
    fs,
    os::unix::fs::{PermissionsExt as _, symlink},
    path::{Path, PathBuf},
};

use tempfile::TempDir;

#[allow(dead_code)]
#[path = "../src/install.rs"]
mod install;

use install::{
    AssetError, InstallError, InstallOutcome, default_bin_dir_from, install_from,
    resolve_integration_asset, stage_archive_root,
};

fn write_file(path: &Path, contents: &[u8], mode: u32) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

fn write_integrations(path: &Path) {
    write_file(
        &path.join("omp/index.js"),
        b"export const omp = true;\n",
        0o644,
    );
    write_file(
        &path.join("omp/package.json"),
        br#"{"type":"module","omp":{"extensions":["./index.js"]}}
"#,
        0o644,
    );
    write_file(
        &path.join("claude-sdk/bridge.js"),
        b"export const bridge = true;\n",
        0o644,
    );
    write_file(
        &path.join("claude-sdk/manifest.json"),
        br#"{"version":1,"entrypoint":"bridge.js"}
"#,
        0o644,
    );
    write_file(
        &path.join("claude-sdk/package.json"),
        br#"{"type":"module","dependencies":{"@anthropic-ai/claude-agent-sdk":"0.3.220"}}
"#,
        0o644,
    );
    write_file(
        &path.join("claude-sdk/node_modules/@anthropic-ai/claude-agent-sdk/index.js"),
        b"export {};\n",
        0o644,
    );
    write_file(
        &path.join("claude-sdk/node_modules/@anthropic-ai/claude-agent-sdk/vendor/tool"),
        b"#!/bin/sh\nexit 0\n",
        0o755,
    );
}

fn write_distribution(root: &Path, binary: &[u8]) -> PathBuf {
    let executable = root.join("bin/asr");
    write_file(&executable, binary, 0o755);
    write_integrations(&root.join("share/agent-session-router/integrations"));
    executable
}

fn staging_entries(parent: &Path) -> Vec<PathBuf> {
    let mut entries = fs::read_dir(parent)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".asr-")
        })
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

#[test]
fn runtime_resolution_never_searches_a_source_checkout() {
    let temporary = TempDir::new().unwrap();
    let checkout = temporary.path().join("checkout");
    let executable = checkout.join("target/debug/asr");
    write_file(&executable, b"binary", 0o755);
    write_file(
        &checkout.join("integrations/omp/index.js"),
        b"checkout-only",
        0o644,
    );

    let result = resolve_integration_asset(&[], &executable, Path::new("omp/index.js"));
    assert!(matches!(result, Err(AssetError::Unavailable)));
}

#[test]
fn explicit_integration_root_has_strict_precedence() {
    let temporary = TempDir::new().unwrap();
    let executable = temporary.path().join("installed/bin/asr");
    write_file(&executable, b"binary", 0o755);
    let installed = temporary
        .path()
        .join("installed/share/agent-session-router/integrations/omp/index.js");
    write_file(&installed, b"installed", 0o644);
    let configured_root = temporary.path().join("configured");
    let configured = configured_root.join("omp/index.js");
    write_file(&configured, b"configured", 0o644);
    let environment = vec![(
        OsString::from("ASR_INTEGRATIONS_DIR"),
        configured_root.as_os_str().to_owned(),
    )];

    assert_eq!(
        resolve_integration_asset(&environment, &executable, Path::new("omp/index.js")).unwrap(),
        configured
    );

    let missing_environment = vec![(
        OsString::from("ASR_INTEGRATIONS_DIR"),
        temporary.path().join("missing").into_os_string(),
    )];
    assert!(matches!(
        resolve_integration_asset(&missing_environment, &executable, Path::new("omp/index.js")),
        Err(AssetError::Unavailable)
    ));
}

#[test]
fn default_bin_directory_uses_home_semantics() {
    let environment = vec![(OsString::from("HOME"), OsString::from("/home/operator"))];
    assert_eq!(
        default_bin_dir_from(&environment).unwrap(),
        Path::new("/home/operator/.local/bin")
    );
    assert!(default_bin_dir_from(&[]).is_none());
}

#[test]
fn archive_staging_uses_native_layout_and_modes() {
    let temporary = TempDir::new().unwrap();
    let source = temporary.path().join("source");
    let executable = write_distribution(&source, b"native-binary");
    let integrations = source.join("share/agent-session-router/integrations");
    let archive_root = temporary.path().join("archive-root");

    stage_archive_root(&executable, &integrations, &archive_root).unwrap();

    assert_eq!(
        fs::read(archive_root.join("bin/asr")).unwrap(),
        b"native-binary"
    );
    assert_eq!(
        fs::symlink_metadata(archive_root.join("bin/asr"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    assert!(
        archive_root
            .join("share/agent-session-router/integrations/omp/index.js")
            .is_file()
    );
    assert!(
        archive_root
            .join("share/agent-session-router/integrations/claude-sdk/bridge.js")
            .is_file()
    );
    assert!(
        archive_root
            .join(
                "share/agent-session-router/integrations/claude-sdk/node_modules/@anthropic-ai/claude-agent-sdk/vendor/tool"
            )
            .is_file()
    );
    let mut top_level = fs::read_dir(&archive_root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    top_level.sort();
    assert_eq!(top_level, [OsString::from("bin"), OsString::from("share")]);
    assert!(staging_entries(temporary.path()).is_empty());
}

#[test]
fn install_is_idempotent_only_for_the_same_binary_and_asset_tree() {
    let temporary = TempDir::new().unwrap();
    let executable = write_distribution(&temporary.path().join("distribution"), b"native-binary");
    let install_prefix = temporary.path().join("installed");
    let bin_dir = install_prefix.join("bin");

    assert_eq!(
        install_from(&executable, &bin_dir).unwrap(),
        InstallOutcome::Installed
    );
    let installed_binary = bin_dir.join("asr");
    assert_eq!(fs::read(&installed_binary).unwrap(), b"native-binary");
    assert_eq!(
        fs::symlink_metadata(&installed_binary)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    assert!(
        install_prefix
            .join("share/agent-session-router/integrations/omp/index.js")
            .is_file()
    );
    assert_eq!(
        install_from(&executable, &bin_dir).unwrap(),
        InstallOutcome::AlreadyInstalled
    );

    fs::write(
        install_prefix.join("share/agent-session-router/integrations/omp/index.js"),
        b"different",
    )
    .unwrap();
    assert!(matches!(
        install_from(&executable, &bin_dir),
        Err(InstallError::Conflict { .. })
    ));
    assert_eq!(fs::read(&installed_binary).unwrap(), b"native-binary");
    assert!(staging_entries(&install_prefix).is_empty());
}

#[test]
fn install_refuses_binary_conflicts_and_all_target_symlinks() {
    let temporary = TempDir::new().unwrap();
    let executable = write_distribution(&temporary.path().join("distribution"), b"native-binary");

    let conflict_prefix = temporary.path().join("conflict");
    let conflict_bin = conflict_prefix.join("bin");
    write_file(&conflict_bin.join("asr"), b"keep-this", 0o755);
    assert!(matches!(
        install_from(&executable, &conflict_bin),
        Err(InstallError::Conflict { .. })
    ));
    assert_eq!(fs::read(conflict_bin.join("asr")).unwrap(), b"keep-this");
    assert!(staging_entries(&conflict_prefix).is_empty());

    let symlink_prefix = temporary.path().join("symlink");
    let symlink_bin = symlink_prefix.join("bin");
    fs::create_dir_all(&symlink_bin).unwrap();
    let unrelated = symlink_prefix.join("unrelated");
    write_file(&unrelated, b"unrelated", 0o755);
    symlink(&unrelated, symlink_bin.join("asr")).unwrap();
    assert!(matches!(
        install_from(&executable, &symlink_bin),
        Err(InstallError::UnsafePath { .. })
    ));
    assert_eq!(fs::read(&unrelated).unwrap(), b"unrelated");
    assert!(staging_entries(&symlink_prefix).is_empty());

    let asset_link_prefix = temporary.path().join("asset-link");
    fs::create_dir_all(&asset_link_prefix).unwrap();
    let outside_share = temporary.path().join("outside-share");
    fs::create_dir_all(&outside_share).unwrap();
    symlink(&outside_share, asset_link_prefix.join("share")).unwrap();
    assert!(matches!(
        install_from(&executable, &asset_link_prefix.join("bin")),
        Err(InstallError::UnsafePath { .. })
    ));
    assert!(!asset_link_prefix.join("bin/asr").exists());
    assert!(fs::read_dir(&outside_share).unwrap().next().is_none());
    assert!(staging_entries(&asset_link_prefix).is_empty());
}

#[test]
fn legacy_python_checkout_symlink_requires_manual_removal() {
    let temporary = TempDir::new().unwrap();
    let executable = write_distribution(&temporary.path().join("distribution"), b"native-binary");
    let install_prefix = temporary.path().join("installed");
    let bin_dir = install_prefix.join("bin");
    fs::create_dir_all(&bin_dir).unwrap();
    let checkout_launcher = temporary.path().join("checkout/scripts/asr.py");
    write_file(&checkout_launcher, b"#!/usr/bin/env python3\n", 0o755);
    let legacy = bin_dir.join("agent-session-router");
    symlink(&checkout_launcher, &legacy).unwrap();

    let error = install_from(&executable, &bin_dir).unwrap_err();
    match error {
        InstallError::SourceCheckoutSymlink { path, target } => {
            assert_eq!(path, legacy);
            assert_eq!(target, checkout_launcher);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(legacy.is_symlink());
    assert!(!bin_dir.join("asr").exists());
    assert!(staging_entries(&install_prefix).is_empty());
}

#[test]
fn source_asset_symlink_is_rejected_and_staging_is_cleaned() {
    let temporary = TempDir::new().unwrap();
    let distribution = temporary.path().join("distribution");
    let executable = write_distribution(&distribution, b"native-binary");
    let integrations = distribution.join("share/agent-session-router/integrations");
    let outside = distribution.join("outside.js");
    write_file(&outside, b"outside", 0o644);
    symlink(&outside, integrations.join("omp/linked.js")).unwrap();
    let install_prefix = temporary.path().join("installed");

    assert!(matches!(
        install_from(&executable, &install_prefix.join("bin")),
        Err(InstallError::UnsafePath { .. })
    ));
    assert!(staging_entries(&install_prefix).is_empty());
    assert!(!install_prefix.join("bin/asr").exists());
    assert!(
        !install_prefix
            .join("share/agent-session-router/integrations")
            .exists()
    );
}

#[test]
fn binary_only_install_does_not_discover_checkout_assets() {
    let temporary = TempDir::new().unwrap();
    let checkout = temporary.path().join("checkout");
    let executable = checkout.join("target/debug/asr");
    write_file(&executable, b"native-binary", 0o755);
    write_integrations(&checkout.join("dist/integrations"));
    let install_prefix = temporary.path().join("installed");

    assert_eq!(
        install_from(&executable, &install_prefix.join("bin")).unwrap(),
        InstallOutcome::Installed
    );
    assert!(install_prefix.join("bin/asr").is_file());
    assert!(
        !install_prefix
            .join("share/agent-session-router/integrations")
            .exists()
    );
}

#[test]
#[ignore = "requires ASR_PACKAGE_BINARY, ASR_PACKAGE_INTEGRATIONS, and ASR_PACKAGE_ROOT"]
fn packaged_archive_contract() {
    let source_binary =
        PathBuf::from(std::env::var_os("ASR_PACKAGE_BINARY").expect("ASR_PACKAGE_BINARY"));
    let source_integrations = PathBuf::from(
        std::env::var_os("ASR_PACKAGE_INTEGRATIONS").expect("ASR_PACKAGE_INTEGRATIONS"),
    );
    let archive_root =
        PathBuf::from(std::env::var_os("ASR_PACKAGE_ROOT").expect("ASR_PACKAGE_ROOT"));

    stage_archive_root(&source_binary, &source_integrations, &archive_root).unwrap();
    let packaged_binary = archive_root.join("bin/asr");
    assert_eq!(
        resolve_integration_asset(&[], &packaged_binary, Path::new("claude-sdk/bridge.js"))
            .unwrap(),
        archive_root.join("share/agent-session-router/integrations/claude-sdk/bridge.js")
    );
    assert_eq!(
        resolve_integration_asset(&[], &packaged_binary, Path::new("omp/index.js")).unwrap(),
        archive_root.join("share/agent-session-router/integrations/omp/index.js")
    );
    assert!(matches!(
        resolve_integration_asset(&[], &packaged_binary, Path::new("claude-sdk/missing.js")),
        Err(AssetError::Unavailable)
    ));

    let installed = TempDir::new().unwrap();
    let installed_bin = installed.path().join("prefix/bin");
    assert_eq!(
        install_from(&packaged_binary, &installed_bin).unwrap(),
        InstallOutcome::Installed
    );
    assert_eq!(
        install_from(&packaged_binary, &installed_bin).unwrap(),
        InstallOutcome::AlreadyInstalled
    );
    assert_eq!(
        resolve_integration_asset(
            &[],
            &installed_bin.join("asr"),
            Path::new("claude-sdk/bridge.js")
        )
        .unwrap(),
        installed
            .path()
            .join("prefix/share/agent-session-router/integrations/claude-sdk/bridge.js")
    );
}
