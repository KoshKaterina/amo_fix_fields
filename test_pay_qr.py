"""Сверка генератора pay_qr с эталоном python-qrcode (уровень M, байтовый режим).

Эталон нужен только тесту: в контейнере qrcode нет, тест там пропускается.
Запуск: python -m pytest test_pay_qr.py
"""
import random
import string

import pytest

import pay_qr

qrcode = pytest.importorskip("qrcode")
import qrcode.constants  # noqa: E402
import qrcode.util  # noqa: E402


def _reference(data, mask=None):
    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M, border=0, mask_pattern=mask)
    qr.add_data(qrcode.util.QRData(data.encode("utf-8"), mode=qrcode.util.MODE_8BIT_BYTE))
    qr.make(fit=True)
    return [[bool(v) for v in row] for row in qr.modules]


def _cases():
    rnd = random.Random(20261126)
    alphabet = string.ascii_letters + string.digits
    cases = [
        "https://qr.nspk.ru/AD10107AGA7BIIL69PAA6FVRC08GR4LE",
        "https://qr.nspk.ru/AD10107AGA7BIIL69PAA6FVRC08GR4LE?type=02&bank=100000000111&sum=159600&cur=RUB&crc=AB12",
        "https://checkout.ozon.ru/order/01a216ce-002f-742c-9255-a37d847301e3",
        "Оплата заказа №10414",
        "",
        "a",
    ]
    # Длины, пересекающие границы версий 1-12 (там переходы счётчика длины и групп блоков).
    for length in (13, 14, 15, 26, 27, 42, 43, 61, 62, 84, 85, 106, 107, 152, 153, 230, 231, 287, 288, 331, 332):
        cases.append("https://qr.nspk.ru/" + "".join(rnd.choice(alphabet) for _ in range(max(0, length - 19))))
    # Большие версии: служебные поля версии (7+) и много блоков.
    for length in (500, 1000, 2000):
        cases.append("".join(rnd.choice(alphabet) for _ in range(length)))
    return cases


@pytest.mark.parametrize("data", _cases())
def test_vse_maski_sovpadayut_s_etalonom(data):
    for mask in range(8):
        assert pay_qr.matrix(data, mask) == _reference(data, mask), (mask, len(data))


@pytest.mark.parametrize("data", _cases())
def test_avtovybor_maski_sovpadaet_s_etalonom(data):
    assert pay_qr.matrix(data) == _reference(data), len(data)


def test_svg_pole_tishiny_i_podpis():
    out = pay_qr.svg("https://qr.nspk.ru/AD10", 'QR "заказ" <1>')
    n = len(pay_qr.matrix("https://qr.nspk.ru/AD10")) + 8
    assert out.startswith('<svg xmlns="http://www.w3.org/2000/svg"')
    assert f'viewBox="0 0 {n} {n}"' in out
    assert "&quot;заказ&quot; &lt;1&gt;" in out and "<1>" not in out
