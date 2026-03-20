from src.price_fetcher import PriceFetcher


def test_normalize_price_payload_for_cn_stock_etf_and_fund():
    fetcher = PriceFetcher(storage=None, use_cache=False)

    cn_payload = fetcher._normalize_price_payload({
        'code': '000001',
        'price': 10.005,
        'prev_close': 9.995,
        'change': 0.015,
        'change_pct': 0.155,
        'currency': 'CNY',
        'cny_price': 10.005,
        'source': 'tencent',
    })
    assert cn_payload['price'] == 10.01
    assert cn_payload['prev_close'] == 10.0
    assert cn_payload['change'] == 0.02
    assert cn_payload['change_pct'] == 0.16
    assert cn_payload['cny_price'] == 10.01

    etf_payload = fetcher._normalize_price_payload({
        'code': '510300',
        'price': 3.335,
        'prev_close': 3.325,
        'change': 0.0101,
        'change_pct': 0.303,
        'currency': 'CNY',
        'cny_price': 3.335,
        'source': 'tencent_etf',
    })
    assert etf_payload['price'] == 3.34
    assert etf_payload['change'] == 0.01
    assert etf_payload['change_pct'] == 0.3

    fund_payload = fetcher._normalize_price_payload({
        'code': '000001',
        'price': 1.23456,
        'change_pct': 0.1234,
        'currency': 'CNY',
        'cny_price': 1.23456,
        'source': 'eastmoney',
    })
    assert fund_payload['price'] == 1.23
    assert fund_payload['change_pct'] == 0.12
    assert fund_payload['cny_price'] == 1.23
