"""S3 helper utilities for the worker."""


def apply_s3_prefix(prefix: str, key: str) -> str:
    """Apply the optional S3 prefix to a key while avoiding duplicates."""
    normalized_prefix = (prefix or "").strip("/")
    normalized_key = (key or "").lstrip("/")
    if not normalized_prefix:
        return normalized_key
    if not normalized_key:
        return normalized_prefix
    if normalized_key == normalized_prefix or normalized_key.startswith(f"{normalized_prefix}/"):
        return normalized_key
    return f"{normalized_prefix}/{normalized_key}"
