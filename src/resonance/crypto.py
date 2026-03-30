"""Fernet-based token encryption utilities for OAuth token storage."""

import cryptography.fernet as fernet_module


class TokenDecryptionError(Exception):
    """Raised when a token cannot be decrypted."""


def encrypt_token(plaintext: str, key: str) -> str:
    """Encrypt a plaintext string using Fernet symmetric encryption."""
    f = fernet_module.Fernet(key.encode())
    return f.encrypt(plaintext.encode()).decode()


def decrypt_token(ciphertext: str, key: str) -> str:
    """Decrypt a Fernet-encrypted string back to plaintext."""
    f = fernet_module.Fernet(key.encode())
    try:
        return f.decrypt(ciphertext.encode()).decode()
    except fernet_module.InvalidToken as exc:
        raise TokenDecryptionError("Failed to decrypt token") from exc
