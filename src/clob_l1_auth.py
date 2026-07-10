"""
clob_l1_auth.py — L1 (wallet) authentication for Polymarket CLOB v2.
Uses py_clob_client_v2.ClobClient to create or derive API credentials.

v2 changes from v1:
  - EIP-712 domain for Exchange changed from version "1" to "2".
  - Auth domain (ClobAuthDomain) stays at version "1".
  - API key creation/derivation is the same endpoint structure.
  - Returns ApiCreds-compatible dataclass.
"""

from dataclasses import dataclass

from loguru import logger

from py_clob_client_v2.signer import Signer
from py_clob_client_v2.signing.eip712 import sign_clob_auth_message


@dataclass
class ClobApiCreds:
    """API credentials returned by L1 auth (api_key, secret, passphrase)."""
    api_key: str
    api_secret: str
    api_passphrase: str

    def to_dict(self) -> dict:
        return {
            "api_key":        self.api_key,
            "api_secret":     self.api_secret,
            "api_passphrase": self.api_passphrase,
        }


class ClobL1Error(Exception):
    """Error during L1 auth / API key creation."""


def _build_l1_headers(signer: Signer, timestamp: int, nonce: int) -> dict:
    """
    Build L1 auth headers (POLY_ADDRESS / POLY_SIGNATURE / POLY_TIMESTAMP /
    POLY_NONCE). The EIP-712 signature is always bound to signer.address()
    (the EOA), so the POLY_ADDRESS header MUST carry the EOA address — for
    Safe/Proxy wallets the EOA (derived from the private key) is the correct
    address for auth.

    B19 fix: removed the address_override parameter — it was never used and
    accepting it implied callers could swap the EOA address, which would
    produce an invalid signature.
    """
    from py_clob_client_v2.headers.headers import (
        POLY_ADDRESS, POLY_SIGNATURE, POLY_TIMESTAMP, POLY_NONCE,
    )

    signature = sign_clob_auth_message(signer, timestamp, nonce)
    return {
        POLY_ADDRESS:   signer.address(),
        POLY_SIGNATURE: signature,
        POLY_TIMESTAMP: str(timestamp),
        POLY_NONCE:     str(nonce),
    }


def _create_api_key_with_headers(host: str, headers: dict) -> ClobApiCreds:
    from py_clob_client_v2.http_helpers.helpers import post
    from py_clob_client_v2.endpoints import CREATE_API_KEY
    resp = post(f"{host}{CREATE_API_KEY}", headers=headers)
    return ClobApiCreds(
        api_key=resp["apiKey"],
        api_secret=resp["secret"],
        api_passphrase=resp["passphrase"],
    )


def _derive_api_key_with_headers(host: str, headers: dict) -> ClobApiCreds:
    from py_clob_client_v2.http_helpers.helpers import get
    from py_clob_client_v2.endpoints import DERIVE_API_KEY
    resp = get(f"{host}{DERIVE_API_KEY}", headers=headers)
    return ClobApiCreds(
        api_key=resp["apiKey"],
        api_secret=resp["secret"],
        api_passphrase=resp["passphrase"],
    )


def derive_clob_api_creds(
    host: str,
    private_key: str,
    chain_id: int = 137,
) -> ClobApiCreds:
    """
    Derive existing API credentials from wallet (nonce=0).
    Raises ClobL1Error on failure.
    """
    try:
        import time
        signer = Signer(private_key, chain_id)
        ts = int(time.time())
        headers = _build_l1_headers(signer, ts, 0)
        creds = _derive_api_key_with_headers(host, headers)
        return ClobApiCreds(
            api_key=creds.api_key,
            api_secret=creds.api_secret,
            api_passphrase=creds.api_passphrase,
        )
    except Exception as exc:
        raise ClobL1Error(f"derive_clob_api_creds failed: {exc}") from exc


def create_clob_api_creds(
    host: str,
    private_key: str,
    chain_id: int = 137,
) -> ClobApiCreds:
    """
    Create new API credentials for wallet.
    Raises ClobL1Error on failure.
    """
    try:
        import time
        signer = Signer(private_key, chain_id)
        ts = int(time.time())
        headers = _build_l1_headers(signer, ts, 0)
        creds = _create_api_key_with_headers(host, headers)
        return ClobApiCreds(
            api_key=creds.api_key,
            api_secret=creds.api_secret,
            api_passphrase=creds.api_passphrase,
        )
    except Exception as exc:
        raise ClobL1Error(f"create_clob_api_creds failed: {exc}") from exc


def create_or_derive_clob_api_creds(
    host: str,
    private_key: str,
    chain_id: int = 137,
) -> ClobApiCreds:
    """
    Create new credentials, falling back to derive if they already exist.
    Recommended for normal wallet connections.
    Raises ClobL1Error if both fail.
    """
    try:
        return create_clob_api_creds(host, private_key, chain_id)
    except ClobL1Error:
        pass

    try:
        return derive_clob_api_creds(host, private_key, chain_id)
    except ClobL1Error as exc:
        raise ClobL1Error(
            f"Both create and derive failed. Ensure wallet has pUSD balance and "
            f"has logged into Polymarket.com at least once. Last error: {exc}"
        ) from exc


def build_l2_auth_headers(
    private_key: str,
    host: str,
    api_key: str,
    api_secret: str,
    api_passphrase: str,
    method: str,
    path: str,
) -> dict:
    """
    Build L2 auth headers for CLOB API requests (requires both L1 signer + L2 HMAC).
    Used for deposit / balance sync which needs full auth.
    """
    import time
    from py_clob_client_v2.clob_types import ApiCreds, RequestArgs
    from py_clob_client_v2.headers.headers import create_level_2_headers

    signer = Signer(private_key, 137)
    creds = ApiCreds(
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
    )
    request_args = RequestArgs(method=method, request_path=path, body=None, serialized_body=None)
    headers = create_level_2_headers(signer, creds, request_args, timestamp=int(time.time()))
    return headers
