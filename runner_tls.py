"""Create and inspect a self-signed server identity for --tls-auto."""

from datetime import datetime, timedelta, timezone
import os

TLS_NAME = "agentrun"


def cert_not_after(cert_path):
    """Return the certificate's UTC expiry datetime, or None if unavailable."""
    try:
        from cryptography import x509
    except ImportError:
        return None
    try:
        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
    except (OSError, ValueError):
        return None
    return getattr(cert, "not_valid_after_utc", None) or cert.not_valid_after.replace(tzinfo=timezone.utc)


def ensure_tls_files(state_dir):
    cert_path = state_dir / "runner.pem"
    key_path = state_dir / "runner-key.pem"
    existing = (cert_path.exists(), key_path.exists())
    if existing == (True, True):
        return cert_path, key_path
    if any(existing):
        raise ValueError(f"incomplete TLS pair in {state_dir}; restore the missing file or move the old file aside")

    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
    except ImportError as exc:
        raise ValueError("TLS setup needs cryptography. Run: python -m pip install --only-binary=:all: cryptography") from exc

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = key.public_key()
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, TLS_NAME)])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(public_key)
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=365))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(TLS_NAME)]), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(public_key), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(public_key), critical=False)
        .sign(key, hashes.SHA256())
    )
    key_pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                serialization.NoEncryption())
    cert_pem = certificate.public_bytes(serialization.Encoding.PEM)

    key_fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(key_fd, "wb") as private_file:
            private_file.write(key_pem)
        with cert_path.open("xb") as public_file:
            public_file.write(cert_pem)
    except OSError:
        key_path.unlink(missing_ok=True)
        raise
    return cert_path, key_path
