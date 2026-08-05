"""Выключатели контура Фулфилмента: по умолчанию включены, гасятся настройкой."""
import importlib
import os

import pytest


def _reload(**env):
    """Перечитывает конфиг с подменённым окружением."""
    old = {k: os.environ.get(k) for k in env}
    os.environ.update({k: v for k, v in env.items() if v is not None})
    for k, v in env.items():
        if v is None:
            os.environ.pop(k, None)
    import waybill_config
    cfg = importlib.reload(waybill_config)
    for k, v in old.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    return cfg


def test_on_by_default():
    """Молча отключить чужой контур выкаткой кода нельзя — по умолчанию всё работает."""
    cfg = _reload(MS_STATUS_SYNC_ENABLED=None, KONTROL_GATE_ENABLED=None)
    assert cfg.MS_STATUS_SYNC_ENABLED
    assert cfg.KONTROL_GATE_ENABLED


def test_off_by_zero():
    cfg = _reload(MS_STATUS_SYNC_ENABLED="0", KONTROL_GATE_ENABLED="0")
    assert not cfg.MS_STATUS_SYNC_ENABLED
    assert not cfg.KONTROL_GATE_ENABLED


def test_explicit_one_keeps_on():
    cfg = _reload(MS_STATUS_SYNC_ENABLED="1", KONTROL_GATE_ENABLED="1")
    assert cfg.MS_STATUS_SYNC_ENABLED
    assert cfg.KONTROL_GATE_ENABLED


@pytest.mark.parametrize("value", ["", " ", "нет"])
def test_only_zero_disables(value):
    """Выключает ровно «0»: случайный мусор в переменной не должен гасить контур."""
    cfg = _reload(MS_STATUS_SYNC_ENABLED=value, KONTROL_GATE_ENABLED=value)
    assert cfg.MS_STATUS_SYNC_ENABLED
    assert cfg.KONTROL_GATE_ENABLED
