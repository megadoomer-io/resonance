import cryptography.fernet as fernet_module
import pytest

import resonance.crypto as crypto_module


@pytest.fixture
def fernet_key() -> str:
    return fernet_module.Fernet.generate_key().decode()


def test_encrypt_returns_different_string(fernet_key: str) -> None:
    plaintext = "my-secret-token"
    encrypted = crypto_module.encrypt_token(plaintext, fernet_key)
    assert encrypted != plaintext


def test_decrypt_recovers_original(fernet_key: str) -> None:
    plaintext = "my-secret-token"
    encrypted = crypto_module.encrypt_token(plaintext, fernet_key)
    decrypted = crypto_module.decrypt_token(encrypted, fernet_key)
    assert decrypted == plaintext


def test_decrypt_with_wrong_key_raises(fernet_key: str) -> None:
    other_key = fernet_module.Fernet.generate_key().decode()
    encrypted = crypto_module.encrypt_token("secret", fernet_key)
    with pytest.raises(crypto_module.TokenDecryptionError):
        crypto_module.decrypt_token(encrypted, other_key)


def test_encrypt_empty_string(fernet_key: str) -> None:
    encrypted = crypto_module.encrypt_token("", fernet_key)
    decrypted = crypto_module.decrypt_token(encrypted, fernet_key)
    assert decrypted == ""
