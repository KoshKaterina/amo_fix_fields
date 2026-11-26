"""QR-код для страницы оплаты (ISO/IEC 18004): байтовый режим, уровень коррекции M, версии 1-40.

Свой генератор, а не библиотека: в контейнере нет ни qrcode, ни segno, а новая зависимость
это лишняя сборка и проверка. Ссылка на оплату не уходит третьим сторонам, QR рисуется на
сервере обычным SVG и виден без JavaScript.

Перенос генератора плагина сайта `sunscrypt-sbp` (includes/class-sunscrypt-sbp-qr.php), который
сверен с python-qrcode помодульно. Таблицы блоков Рида-Соломона и центров выравнивающих узоров
сняты с python-qrcode. Сверка этого переноса: test_pay_qr.py (нужен qrcode только для теста).
"""
from __future__ import annotations

import html

# Блоки для уровня M: (количество блоков, всего кодовых слов в блоке, из них данных).
RS_M = {
    1: [(1, 26, 16)], 2: [(1, 44, 28)], 3: [(1, 70, 44)], 4: [(2, 50, 32)],
    5: [(2, 67, 43)], 6: [(4, 43, 27)], 7: [(4, 49, 31)],
    8: [(2, 60, 38), (2, 61, 39)], 9: [(3, 58, 36), (2, 59, 37)],
    10: [(4, 69, 43), (1, 70, 44)], 11: [(1, 80, 50), (4, 81, 51)],
    12: [(6, 58, 36), (2, 59, 37)], 13: [(8, 59, 37), (1, 60, 38)],
    14: [(4, 64, 40), (5, 65, 41)], 15: [(5, 65, 41), (5, 66, 42)],
    16: [(7, 73, 45), (3, 74, 46)], 17: [(10, 74, 46), (1, 75, 47)],
    18: [(9, 69, 43), (4, 70, 44)], 19: [(3, 70, 44), (11, 71, 45)],
    20: [(3, 67, 41), (13, 68, 42)], 21: [(17, 68, 42)], 22: [(17, 74, 46)],
    23: [(4, 75, 47), (14, 76, 48)], 24: [(6, 73, 45), (14, 74, 46)],
    25: [(8, 75, 47), (13, 76, 48)], 26: [(19, 74, 46), (4, 75, 47)],
    27: [(22, 73, 45), (3, 74, 46)], 28: [(3, 73, 45), (23, 74, 46)],
    29: [(21, 73, 45), (7, 74, 46)], 30: [(19, 75, 47), (10, 76, 48)],
    31: [(2, 74, 46), (29, 75, 47)], 32: [(10, 74, 46), (23, 75, 47)],
    33: [(14, 74, 46), (21, 75, 47)], 34: [(14, 74, 46), (23, 75, 47)],
    35: [(12, 75, 47), (26, 76, 48)], 36: [(6, 75, 47), (34, 76, 48)],
    37: [(29, 74, 46), (14, 75, 47)], 38: [(13, 74, 46), (32, 75, 47)],
    39: [(40, 75, 47), (7, 76, 48)], 40: [(18, 75, 47), (31, 76, 48)],
}

# Центры выравнивающих узоров по версиям.
ALIGN = {
    1: [], 2: [6, 18], 3: [6, 22], 4: [6, 26], 5: [6, 30], 6: [6, 34],
    7: [6, 22, 38], 8: [6, 24, 42], 9: [6, 26, 46], 10: [6, 28, 50],
    11: [6, 30, 54], 12: [6, 32, 58], 13: [6, 34, 62],
    14: [6, 26, 46, 66], 15: [6, 26, 48, 70], 16: [6, 26, 50, 74],
    17: [6, 30, 54, 78], 18: [6, 30, 56, 82], 19: [6, 30, 58, 86], 20: [6, 34, 62, 90],
    21: [6, 28, 50, 72, 94], 22: [6, 26, 50, 74, 98], 23: [6, 30, 54, 78, 102],
    24: [6, 28, 54, 80, 106], 25: [6, 32, 58, 84, 110], 26: [6, 30, 58, 86, 114],
    27: [6, 34, 62, 90, 118],
    28: [6, 26, 50, 74, 98, 122], 29: [6, 30, 54, 78, 102, 126], 30: [6, 26, 52, 78, 104, 130],
    31: [6, 30, 56, 82, 108, 134], 32: [6, 34, 60, 86, 112, 138], 33: [6, 30, 58, 86, 114, 142],
    34: [6, 34, 62, 90, 118, 146],
    35: [6, 30, 54, 78, 102, 126, 150], 36: [6, 24, 50, 76, 102, 128, 154],
    37: [6, 28, 54, 80, 106, 132, 158], 38: [6, 32, 58, 84, 110, 136, 162],
    39: [6, 26, 54, 82, 110, 138, 166], 40: [6, 30, 58, 86, 114, 142, 170],
}

_EXP = [0] * 512
_LOG = [0] * 256
_x = 1
for _i in range(255):
    _EXP[_i] = _x
    _LOG[_x] = _i
    _x <<= 1
    if _x & 0x100:
        _x ^= 0x11D
for _i in range(255, 512):
    _EXP[_i] = _EXP[_i - 255]


def _gf_mul(a: int, b: int) -> int:
    if a == 0 or b == 0:
        return 0
    return _EXP[_LOG[a] + _LOG[b]]


def _data_capacity(version: int) -> int:
    return sum(count * data for count, _total, data in RS_M[version])


def _pick_version(length: int) -> int:
    for v in range(1, 41):
        bits = 4 + (8 if v < 10 else 16) + 8 * length
        if bits <= _data_capacity(v) * 8:
            return v
    raise ValueError("QR: данные не помещаются")


def _put(bits: list[int], value: int, length: int) -> None:
    for i in range(length - 1, -1, -1):
        bits.append((value >> i) & 1)


def _rs_remainder(block: list[int], ec_count: int) -> list[int]:
    gen = [1]
    for i in range(ec_count):
        nxt = [0] * (len(gen) + 1)
        for j, coef in enumerate(gen):
            nxt[j] ^= coef
            nxt[j + 1] ^= _gf_mul(coef, _EXP[i])
        gen = nxt
    res = block + [0] * ec_count
    for i in range(len(block)):
        coef = res[i]
        if coef == 0:
            continue
        for j in range(1, ec_count + 1):
            res[i + j] ^= _gf_mul(gen[j], coef)
    return res[len(block):]


def _codewords(data_bytes: bytes, version: int) -> list[int]:
    """Данные + коррекция, перемешанные по блокам."""
    capacity = _data_capacity(version)
    bits: list[int] = []
    _put(bits, 0b0100, 4)
    _put(bits, len(data_bytes), 8 if version < 10 else 16)
    for b in data_bytes:
        _put(bits, b, 8)
    if len(bits) + 4 <= capacity * 8:
        _put(bits, 0, 4)
    while len(bits) % 8:
        bits.append(0)
    data = []
    for i in range(0, len(bits), 8):
        byte = 0
        for j in range(8):
            byte = (byte << 1) | bits[i + j]
        data.append(byte)
    pad = (0xEC, 0x11)
    i = 0
    while len(data) < capacity:
        data.append(pad[i % 2])
        i += 1

    data_blocks: list[list[int]] = []
    ec_blocks: list[list[int]] = []
    offset = 0
    for count, total, data_count in RS_M[version]:
        for _ in range(count):
            block = data[offset:offset + data_count]
            offset += data_count
            data_blocks.append(block)
            ec_blocks.append(_rs_remainder(block, total - data_count))
    out: list[int] = []
    for blocks in (data_blocks, ec_blocks):
        longest = max(len(b) for b in blocks)
        for i in range(longest):
            for b in blocks:
                if i < len(b):
                    out.append(b[i])
    return out


def _bch_digit(data: int) -> int:
    return data.bit_length()


def _bch_format(data: int) -> int:
    g = 0b10100110111
    d = data << 10
    while _bch_digit(d) - _bch_digit(g) >= 0:
        d ^= g << (_bch_digit(d) - _bch_digit(g))
    return ((data << 10) | d) ^ 0b101010000010010


def _bch_version(data: int) -> int:
    g = 0b1111100100101
    d = data << 12
    while _bch_digit(d) - _bch_digit(g) >= 0:
        d ^= g << (_bch_digit(d) - _bch_digit(g))
    return (data << 12) | d


def _mask_bit(mask: int, i: int, j: int) -> bool:
    if mask == 0:
        return (i + j) % 2 == 0
    if mask == 1:
        return i % 2 == 0
    if mask == 2:
        return j % 3 == 0
    if mask == 3:
        return (i + j) % 3 == 0
    if mask == 4:
        return (i // 2 + j // 3) % 2 == 0
    if mask == 5:
        return (i * j) % 2 + (i * j) % 3 == 0
    if mask == 6:
        return ((i * j) % 2 + (i * j) % 3) % 2 == 0
    return ((i * j) % 3 + (i + j) % 2) % 2 == 0


def _build(words: list[int], version: int, mask: int, test: bool) -> list[list[bool]]:
    """Сборка символа. test — служебные поля пустые (для подсчёта штрафа, как в эталоне)."""
    n = 17 + 4 * version
    m: list[list] = [[None] * n for _ in range(n)]

    for row, col in ((0, 0), (n - 7, 0), (0, n - 7)):
        for r in range(-1, 8):
            for c in range(-1, 8):
                if not (0 <= row + r < n and 0 <= col + c < n):
                    continue
                m[row + r][col + c] = (
                    (0 <= r <= 6 and c in (0, 6))
                    or (0 <= c <= 6 and r in (0, 6))
                    or (2 <= r <= 4 and 2 <= c <= 4)
                )

    pos = ALIGN[version]
    for row in pos:
        for col in pos:
            if m[row][col] is not None:
                continue
            for r in range(-2, 3):
                for c in range(-2, 3):
                    m[row + r][col + c] = abs(r) == 2 or abs(c) == 2 or (r == 0 and c == 0)

    for i in range(8, n - 8):
        if m[i][6] is None:
            m[i][6] = i % 2 == 0
        if m[6][i] is None:
            m[6][i] = i % 2 == 0

    bits = _bch_format(mask)  # уровень M = 00, поэтому старшие биты нулевые
    for i in range(15):
        mod = (not test) and ((bits >> i) & 1) == 1
        if i < 6:
            m[i][8] = mod
        elif i < 8:
            m[i + 1][8] = mod
        else:
            m[n - 15 + i][8] = mod
    for i in range(15):
        mod = (not test) and ((bits >> i) & 1) == 1
        if i < 8:
            m[8][n - i - 1] = mod
        elif i < 9:
            m[8][15 - i] = mod
        else:
            m[8][15 - i - 1] = mod
    m[n - 8][8] = not test

    if version >= 7:
        vbits = _bch_version(version)
        for i in range(18):
            mod = (not test) and ((vbits >> i) & 1) == 1
            m[i // 3][i % 3 + n - 11] = mod
            m[i % 3 + n - 11][i // 3] = mod

    inc = -1
    row = n - 1
    bit_index = 7
    byte_index = 0
    length = len(words)
    col = n - 1
    while col > 0:
        if col == 6:
            col -= 1
        while True:
            for c in (col, col - 1):
                if m[row][c] is not None:
                    continue
                dark = byte_index < length and ((words[byte_index] >> bit_index) & 1) == 1
                if _mask_bit(mask, row, c):
                    dark = not dark
                m[row][c] = dark
                bit_index -= 1
                if bit_index == -1:
                    byte_index += 1
                    bit_index = 7
            row += inc
            if row < 0 or row >= n:
                row -= inc
                inc = -inc
                break
        col -= 2
    return m


def _penalty(m: list[list[bool]]) -> int:
    """Штраф маски — четыре правила стандарта, в той же арифметике, что у эталона."""
    n = len(m)
    runs = [0] * (n + 1)
    for a in range(2):
        for i in range(n):
            prev = m[i][0] if a == 0 else m[0][i]
            length = 0
            for j in range(n):
                cur = m[i][j] if a == 0 else m[j][i]
                if cur == prev:
                    length += 1
                else:
                    if length >= 5:
                        runs[length] += 1
                    length = 1
                    prev = cur
            if length >= 5:
                runs[length] += 1
    score = sum(runs[l] * (l - 2) for l in range(5, n + 1))

    for r in range(n - 1):
        for c in range(n - 1):
            v = m[r][c]
            if m[r][c + 1] == v and m[r + 1][c] == v and m[r + 1][c + 1] == v:
                score += 3

    p1 = (True, False, True, True, True, False, True, False, False, False, False)
    p2 = tuple(reversed(p1))
    for a in range(2):
        for i in range(n):
            for j in range(n - 10):
                hit1 = hit2 = True
                for k in range(11):
                    if not (hit1 or hit2):
                        break
                    v = m[i][j + k] if a == 0 else m[j + k][i]
                    hit1 = hit1 and v == p1[k]
                    hit2 = hit2 and v == p2[k]
                if hit1 or hit2:
                    score += 40

    dark = sum(1 for row in m for v in row if v)
    score += int(abs(dark / (n * n) * 100 - 50) / 5) * 10
    return score


def matrix(data: str, mask: int | None = None) -> list[list[bool]]:
    """Матрица модулей без поля тишины: True — тёмный модуль.
    mask=None — маска по штрафам стандарта, 0-7 — заданная (для сверки)."""
    data_bytes = data.encode("utf-8")
    version = _pick_version(len(data_bytes))
    words = _codewords(data_bytes, version)
    if mask is None:
        best = None
        for candidate in range(8):
            # Как в эталоне: штраф считаем с пустыми (светлыми) служебными полями.
            penalty = _penalty(_build(words, version, candidate, True))
            if best is None or penalty < best[0]:
                best = (penalty, candidate)
        mask = best[1]
    return _build(words, version, mask, False)


def svg(data: str, title: str = "") -> str:
    """SVG с полем тишины в 4 модуля. Размер задаёт CSS, viewBox — в модулях."""
    m = matrix(data)
    n = len(m)
    q = 4
    size = n + 2 * q
    parts = []
    for r in range(n):
        c = 0
        while c < n:
            if not m[r][c]:
                c += 1
                continue
            start = c
            while c < n and m[r][c]:
                c += 1
            parts.append(f"M{start + q} {r + q}h{c - start}v1h-{c - start}z")
    label = html.escape(title, quote=True)
    head = f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" shape-rendering="crispEdges" role="img"'
    if label:
        head += f' aria-label="{label}"'
    return (head + ">" + (f"<title>{label}</title>" if label else "")
            + f'<rect width="{size}" height="{size}" fill="#fff"/>'
            + f'<path d="{"".join(parts)}" fill="#000"/></svg>')
