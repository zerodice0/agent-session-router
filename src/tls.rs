use std::{
    env,
    fs::File,
    io::{BufReader, Cursor, Read},
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};

use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use rustls_pemfile::Item;
use thiserror::Error;

use crate::credentials::open_private_file;

const MAX_CA_FILE_BYTES: usize = 1024 * 1024;

static NATIVE_ROOTS: LazyLock<Result<RootCertStore, TlsError>> = LazyLock::new(|| {
    let loaded = rustls_native_certs::load_native_certs();
    if !loaded.errors.is_empty() {
        return Err(TlsError::NativeRoots);
    }
    let mut roots = RootCertStore::empty();
    for certificate in loaded.certs {
        roots.add(certificate).map_err(|_| TlsError::NativeRoots)?;
    }
    Ok(roots)
});

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TlsError {
    #[error("configuration_required")]
    EnvironmentOverride,
    #[error("tls_root_load_failed")]
    NativeRoots,
    #[error("tls_ca_invalid")]
    InvalidCa,
    #[error("tls_trust_empty")]
    EmptyTrustStore,
    #[error("tls_provider_unavailable")]
    Provider,
    #[error("tls_certificate_invalid")]
    InvalidCertificate,
    #[error("tls_private_key_invalid")]
    InvalidPrivateKey,
}

/// Builds the single rustls client policy used by router WebSocket and HTTP clients.
///
/// Native roots are loaded once. `ASR_CA_FILE`, or the explicit path when one is
/// supplied, is appended using strict certificate-only PEM parsing.
///
/// # Errors
///
/// Returns an error when process-wide certificate override variables are set,
/// native roots cannot be loaded completely, the additional CA is invalid, or
/// no trust anchor remains.
pub fn load_client_config(explicit_ca_file: Option<&Path>) -> Result<Arc<ClientConfig>, TlsError> {
    let mut roots = client_roots()?;
    let ca_file = explicit_ca_file.map(Path::to_path_buf).or_else(asr_ca_file);
    if let Some(path) = ca_file {
        append_ca_file(&mut roots, &path)?;
    }
    client_config(roots)
}

/// Uses already-validated public CA bytes without reopening a persisted pathname.
/// Native trust and environment-override policy are identical to file-based clients.
pub(crate) fn load_client_config_with_ca_pem(ca_pem: &[u8]) -> Result<Arc<ClientConfig>, TlsError> {
    let mut roots = client_roots()?;
    append_ca_bytes(&mut roots, ca_pem)?;
    client_config(roots)
}

fn client_roots() -> Result<RootCertStore, TlsError> {
    install_crypto_provider()?;
    if env::var_os("SSL_CERT_FILE").is_some() || env::var_os("SSL_CERT_DIR").is_some() {
        return Err(TlsError::EnvironmentOverride);
    }
    native_roots()
}

fn client_config(roots: RootCertStore) -> Result<Arc<ClientConfig>, TlsError> {
    if roots.is_empty() {
        return Err(TlsError::EmptyTrustStore);
    }
    Ok(Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

pub fn load_server_config(
    certificate_file: &Path,
    private_key_file: &Path,
) -> Result<Arc<ServerConfig>, TlsError> {
    install_crypto_provider()?;
    let certificate_bytes =
        read_server_file(File::open(certificate_file), TlsError::InvalidCertificate)?;
    let private_key_bytes = read_server_file(
        open_private_file(private_key_file),
        TlsError::InvalidPrivateKey,
    )?;
    if !strict_pem(&certificate_bytes, &["CERTIFICATE"]) {
        return Err(TlsError::InvalidCertificate);
    }
    if !strict_pem(
        &private_key_bytes,
        &["PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY"],
    ) {
        return Err(TlsError::InvalidPrivateKey);
    }

    let mut certificates = Vec::<CertificateDer<'static>>::new();
    let mut certificate_reader = BufReader::new(Cursor::new(certificate_bytes));
    loop {
        match rustls_pemfile::read_one(&mut certificate_reader)
            .map_err(|_| TlsError::InvalidCertificate)?
        {
            Some(Item::X509Certificate(certificate)) => certificates.push(certificate),
            Some(_) => return Err(TlsError::InvalidCertificate),
            None => break,
        }
    }
    if certificates.is_empty() {
        return Err(TlsError::InvalidCertificate);
    }

    let mut private_key = None;
    let mut private_key_reader = BufReader::new(Cursor::new(private_key_bytes));
    loop {
        let item = rustls_pemfile::read_one(&mut private_key_reader)
            .map_err(|_| TlsError::InvalidPrivateKey)?;
        let Some(item) = item else {
            break;
        };
        let parsed = match item {
            Item::Pkcs1Key(key) => PrivateKeyDer::Pkcs1(key),
            Item::Pkcs8Key(key) => PrivateKeyDer::Pkcs8(key),
            Item::Sec1Key(key) => PrivateKeyDer::Sec1(key),
            _ => return Err(TlsError::InvalidPrivateKey),
        };
        if private_key.replace(parsed).is_some() {
            return Err(TlsError::InvalidPrivateKey);
        }
    }
    let private_key = private_key.ok_or(TlsError::InvalidPrivateKey)?;
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map_err(|_| TlsError::InvalidCertificate)?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// Installs the process-wide ring provider exactly once.
///
/// # Errors
///
/// Returns an error only when installation fails and no provider is available.
pub fn install_crypto_provider() -> Result<(), TlsError> {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return Ok(());
    }
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_ok()
        || rustls::crypto::CryptoProvider::get_default().is_some()
    {
        Ok(())
    } else {
        Err(TlsError::Provider)
    }
}

fn read_server_file<E>(file: Result<File, E>, invalid: TlsError) -> Result<Vec<u8>, TlsError> {
    let file = file.map_err(|_| invalid)?;
    let mut bytes = Vec::new();
    file.take((MAX_CA_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid)?;
    if bytes.is_empty() || bytes.len() > MAX_CA_FILE_BYTES {
        return Err(invalid);
    }
    Ok(bytes)
}

fn strict_pem(bytes: &[u8], allowed_labels: &[&str]) -> bool {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return false;
    };
    let envelopes = allowed_labels
        .iter()
        .map(|label| {
            (
                format!("-----BEGIN {label}-----"),
                format!("-----END {label}-----"),
            )
        })
        .collect::<Vec<_>>();
    let mut active: Option<usize> = None;
    let mut payload = false;
    let mut blocks = 0_usize;
    for line in text.lines() {
        if let Some(index) = active {
            if line == envelopes[index].1 {
                if !payload {
                    return false;
                }
                active = None;
                payload = false;
                blocks += 1;
            } else if line.is_empty()
                || !line.bytes().all(|value| {
                    value.is_ascii_alphanumeric() || matches!(value, b'+' | b'/' | b'=')
                })
            {
                return false;
            } else {
                payload = true;
            }
        } else if !line.trim().is_empty() {
            let Some(index) = envelopes.iter().position(|envelope| line == envelope.0) else {
                return false;
            };
            active = Some(index);
        }
    }
    active.is_none() && blocks > 0
}

fn native_roots() -> Result<RootCertStore, TlsError> {
    NATIVE_ROOTS.clone()
}

fn asr_ca_file() -> Option<PathBuf> {
    env::var_os("ASR_CA_FILE")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn append_ca_file(roots: &mut RootCertStore, path: &Path) -> Result<(), TlsError> {
    let file = File::open(path).map_err(|_| TlsError::InvalidCa)?;
    let mut bytes = Vec::new();
    file.take((MAX_CA_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| TlsError::InvalidCa)?;
    append_ca_bytes(roots, &bytes)
}

fn append_ca_bytes(roots: &mut RootCertStore, bytes: &[u8]) -> Result<(), TlsError> {
    if bytes.is_empty() || bytes.len() > MAX_CA_FILE_BYTES {
        return Err(TlsError::InvalidCa);
    }

    let mut reader = BufReader::new(Cursor::new(bytes));
    let mut added = 0_usize;
    loop {
        match rustls_pemfile::read_one(&mut reader).map_err(|_| TlsError::InvalidCa)? {
            Some(Item::X509Certificate(certificate)) => {
                roots.add(certificate).map_err(|_| TlsError::InvalidCa)?;
                added += 1;
            }
            Some(_) => return Err(TlsError::InvalidCa),
            None => break,
        }
    }
    if added == 0 {
        return Err(TlsError::InvalidCa);
    }
    Ok(())
}
