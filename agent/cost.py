"""Athena cost calculation.

Athena pricing (eu-central-1): $5.00 per TB of data scanned.
AWS applies a 10 MB minimum charge per query — even a 1-byte scan
costs as much as a 10 MB scan.

Reference:
  https://aws.amazon.com/athena/pricing/

This module is intentionally tiny. It exists so the conversion constant
lives in exactly one place and every other module imports it rather than
hard-coding the price.
"""

from typing import Final

# Athena price per byte (USD).
# $5.00 / (1024^4 bytes per TB) = ~4.547e-12 USD per byte.
_PRICE_PER_BYTE: Final[float] = 5.00 / (1024**4)

# AWS bills a minimum of 10 MB per query regardless of actual scan size.
_MIN_BYTES: Final[int] = 10 * 1024 * 1024  # 10 MB in bytes


def bytes_to_usd(bytes_scanned: int) -> float:
    """Convert Athena DataScannedInBytes to a USD cost estimate.

    Applies the 10 MB minimum billing floor that AWS charges per query.
    The result is rounded to 6 decimal places — enough precision for
    audit log storage without spurious floating-point noise.

    Args:
        bytes_scanned: The DataScannedInBytes value from the Athena
            QueryExecutionStatistics response. Must be >= 0.

    Returns:
        Estimated cost in USD, with the 10 MB minimum floor applied.

    Examples:
        >>> bytes_to_usd(0)          # charged as 10 MB minimum
        4.6e-06  (approximately)
        >>> bytes_to_usd(5_000_000)  # 5 MB — still charged as 10 MB
        4.6e-06  (approximately)
        >>> bytes_to_usd(104_857_600)  # 100 MB — above minimum
        4.6e-04  (approximately)
    """
    if bytes_scanned < 0:
        raise ValueError(
            f"bytes_scanned must be >= 0, got {bytes_scanned}."
        )
    billable = max(bytes_scanned, _MIN_BYTES)
    return round(billable * _PRICE_PER_BYTE, 6)
