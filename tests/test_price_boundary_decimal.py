from datetime import datetime, timedelta
from pathlib import Path

from src.local_cache import LocalPriceCache
from src.price_fetcher import PriceFetcher
from src.models import PriceCache, AssetType


def test_price_fetcher_normalizes_runtime_price_payload():
    fetcher = PriceFetcher(storage=None, use_cache=False)
    payload = fetcher._normalize_price_payload({
        'code': 'AAPL',
        'price': 123.456,
        'prev_close': 122.221,
        'change': 1.235,
        'change_pct': 1.005,
        'currency': 'USD',
        'cny_price': 888.8888,
        'exchange_rate': 7.1234567,
    })

    assert payload['price'] == 123.46
    assert payload['prev_close'] == 122.22
    assert payload['change'] == 1.24
    assert payload['change_pct'] == 1.01
    assert payload['cny_price'] == 888.89
    assert payload['exchange_rate'] == 7.123457


def test_local_price_cache_save_revalidates_price_payload(tmp_path: Path):
    cache = LocalPriceCache(cache_file=tmp_path / 'price_cache.json')
    price = PriceCache(
        asset_id='AAPL',
        asset_name='Apple',
        asset_type=AssetType.US_STOCK,
        price=123.456,
        currency='USD',
        cny_price=888.8888,
        change=1.235,
        change_pct=1.005,
        exchange_rate=7.1234567,
        data_source='test',
        expires_at=datetime.now() + timedelta(minutes=5),
    )

    cache.save(price, _flush=True)
    loaded = cache.get('AAPL')

    assert loaded is not None
    assert loaded.price == 123.46
    assert loaded.cny_price == 888.89
