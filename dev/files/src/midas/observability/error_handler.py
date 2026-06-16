from __future__ import annotations


def normalize_exception(exc: Exception, max_chars: int = 4000) -> dict[str, str]:
    """Convierte una excepción en un payload controlado para logging."""
    message = str(exc)
    if len(message) > max_chars:
        message = message[:max_chars] + "... [truncated]"
    return {
        "error_code": exc.__class__.__name__,
        "error_message": message,
    }
