#!/usr/bin/env python3

from __future__ import annotations


def test_validate_code_strips_market_suffix_and_normalizes_hk():
    from src.asset_utils import validate_code

    assert validate_code('FUTU.US') == 'FUTU'
    assert validate_code('0700.HK') == '00700'
    assert validate_code('700.HK') == '00700'
    assert validate_code('HK700') == '00700'
    assert validate_code('600519.SH') == '600519'
    assert validate_code('000001.SZ') == '000001'


def test_detect_market_type_respects_suffix():
    from src.asset_utils import detect_market_type

    assert detect_market_type('FUTU.US') == 'us'
    assert detect_market_type('0700.HK') == 'hk'
    assert detect_market_type('600519.SH') == 'cn'


if __name__ == '__main__':
    test_validate_code_strips_market_suffix_and_normalizes_hk()
    test_detect_market_type_respects_suffix()
    print('OK')
