"""Write guard & validation utilities.

Goal: prevent bad / partial inputs from being written to Feishu.

Principles:
- Missing is NOT zero.
- For money-impacting fields, require explicit values.
- Compute derived fields deterministically.

This module is intentionally lightweight and can be imported from skill_api.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Dict, Any, List


@dataclass
class ValidationError:
    field: str
    message: str


def _d(x) -> Optional[Decimal]:
    if x is None:
        return None
    try:
        return Decimal(str(x))
    except Exception:
        return None


def validate_and_normalize_trade_input(
    *,
    tx_type: str,
    quantity,
    price,
    fee=0,
    allow_fee_default_zero: bool = True,
) -> Dict[str, Any]:
    """Validate trade inputs (BUY/SELL) and compute derived fields."""
    errors: List[ValidationError] = []

    q = _d(quantity)
    p = _d(price)

    if q is None:
        errors.append(ValidationError('quantity', 'required'))
    elif q <= 0:
        errors.append(ValidationError('quantity', 'must be > 0'))

    if p is None:
        errors.append(ValidationError('price', 'required'))
    elif p <= 0:
        errors.append(ValidationError('price', 'must be > 0'))

    f = _d(fee)
    if f is None:
        if allow_fee_default_zero:
            f = Decimal('0')
        else:
            errors.append(ValidationError('fee', 'required'))
    elif f < 0:
        errors.append(ValidationError('fee', 'must be >= 0'))

    if errors:
        return {
            'ok': False,
            'errors': [e.__dict__ for e in errors],
            'normalized': None,
        }

    amount = (q * p)

    return {
        'ok': True,
        'errors': [],
        'normalized': {
            'tx_type': tx_type,
            'quantity': float(q),
            'price': float(p),
            'fee': float(f),
            'amount': float(amount),
        }
    }


def validate_and_normalize_nav_input(*, nav, shares, status: str | None = None) -> Dict[str, Any]:
    """Validate NAV record input.

    We must distinguish "missing" from "explicit zero".

    Modes:
    - status is None or 'OPEN': normal mode
        - nav must be provided and > 0
        - shares must be provided and > 0
    - status == 'CLOSED': explicit close/clear mode
        - shares must be provided and == 0
        - nav may be None; we normalize it to 1.0 by default (stable downstream semantics)

    NOTE:
    - shares=None is never allowed (missing ≠ 0).
    - If you want a different closed-nav convention, change normalization here.
    """
    errors: List[ValidationError] = []

    st = (status or 'OPEN').upper()
    n = _d(nav)
    s = _d(shares)

    if s is None:
        errors.append(ValidationError('shares', 'required'))

    if st == 'CLOSED':
        # explicit close semantics
        if s is not None and s != 0:
            errors.append(ValidationError('shares', 'must be 0 when status=CLOSED'))
        if n is None:
            n = Decimal('1.0')
        elif n <= 0:
            errors.append(ValidationError('nav', 'must be > 0'))
    else:
        # OPEN
        if n is None:
            errors.append(ValidationError('nav', 'required'))
        elif n <= 0:
            errors.append(ValidationError('nav', 'must be > 0'))

        if s is not None and s <= 0:
            errors.append(ValidationError('shares', 'must be > 0'))

    if errors:
        return {
            'ok': False,
            'errors': [e.__dict__ for e in errors],
            'normalized': None,
        }

    return {
        'ok': True,
        'errors': [],
        'normalized': {
            'status': st,
            'nav': float(n) if n is not None else None,
            'shares': float(s) if s is not None else None,
        }
    }
