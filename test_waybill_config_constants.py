"""Сторож констант waybill_config: одна сущность — одно имя.

Повод (07.09.2026): воронка «ОП розница» 10593102 четыре месяца жила в конфиге
дважды — `PIPELINE_CLEVER` в блоке воронок и `PIPELINE_CLEVER_MAIN` в блоке Ozon.
Вторую завели через месяц после первой, старую не заметили. Правку делали в одной
константе, вторая молча оставалась прежней; поймали случайно.

Файл разбирается через `ast`, БЕЗ импорта waybill_config: тест не должен зависеть
ни от переменных окружения, ни от порядка импортов. Прогон:

    python3 -m pytest test_waybill_config_constants.py -q

Три сторожа с разной строгостью:

* `PIPELINE_*` — жёсткий запрет. Дублей нет и быть не должно: воронок мало, каждая
  заводится осознанно.
* `STATUS_*` — храповик. Два живых дубля уже есть (см. KNOWN_STATUS_DUPES), их
  разбор — отдельная задача. Тест падает и на НОВОМ дубле, и когда старый починят:
  во втором случае надо обновить список, и это правильно — он должен таять.
* повторное присваивание одного имени — тоже храповик, случай опаснее прочих:
  правят одну строку, а выигрывает молча вторая.

⚠️ `FIELD_*` в сторож НЕ берём осознанно. Там дубли — законные алиасы одного поля
под разные роли: 576719 это и «Адрес доставки», и код ПВЗ; 577415 — и «Номер
заказа на сайте», и номер упаковки; 571657 — и номер заказа СДЭК, и трек ФФ. Имена
разные, потому что смысл в разных контурах разный. Не «улучшайте» тест, добавив их
сюда, — получите красный прогон на ровном месте.
"""

import ast
import collections
import os

CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "waybill_config.py")

# Этапы, живущие под двумя именами. Разбирать — отдельной задачей, здесь только
# фиксируем, чтобы список не рос. Снял дубль — вычеркни строку отсюда.
KNOWN_STATUS_DUPES = {
    75426858: {"STATUS_OFFICE_DEFERRED_RESERVE", "STATUS_OFFICE_RESERVE"},
    83537714: {"STATUS_CLEVER_NEW_LEAD", "STATUS_NEW_LEAD"},
    83953914: {"STATUS_OFFICE_PREORDER_PAID"},  # одно имя, два присваивания — см. ниже
}

# Имена, присвоенные в файле больше одного раза. Второе присваивание молча
# перекрывает первое, читатель этого не видит.
KNOWN_REDEFINED = {"STATUS_OFFICE_PREORDER_PAID"}


def _int_assignments():
    """{имя: [строки]} и {значение: {имя, ...}} по целочисленным константам модуля."""
    tree = ast.parse(open(CONFIG, encoding="utf-8").read())
    lines = collections.defaultdict(list)
    by_value = collections.defaultdict(set)
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        value = node.value
        for target in node.targets:
            if not isinstance(target, ast.Name):
                continue
            lines[target.id].append(node.lineno)
            if isinstance(value, ast.Constant) and isinstance(value.value, int) \
                    and not isinstance(value.value, bool):
                by_value[value.value].add(target.id)
    return lines, by_value


def _dupes(by_value, prefix):
    out = {}
    for value, names in by_value.items():
        same = {n for n in names if n.startswith(prefix)}
        if len(same) > 1:
            out[value] = same
    return out


def test_no_duplicate_pipeline_constants():
    """Две константы PIPELINE_* с одним значением — та самая мина 07.09.2026."""
    _, by_value = _int_assignments()
    dupes = _dupes(by_value, "PIPELINE_")
    assert not dupes, (
        "воронка объявлена дважды под разными именами: "
        + "; ".join(f"{v} → {sorted(n)}" for v, n in sorted(dupes.items()))
        + ". Все воронки живут в блоке «Воронки amoCRM» наверху файла, "
          "в своём разделе оставляйте комментарий-указатель."
    )


def test_status_duplicates_do_not_grow():
    """Храповик: новый дубль этапа не заводим, старые ждут своей задачи."""
    _, by_value = _int_assignments()
    dupes = {v: n for v, n in _dupes(by_value, "STATUS_").items()}
    known = {v: n for v, n in KNOWN_STATUS_DUPES.items() if len(n) > 1}
    assert dupes == known, (
        f"список дублей этапов изменился.\nсейчас: {dupes}\nожидалось: {known}\n"
        "Новый дубль — заведите одно имя. Починили старый — обновите KNOWN_STATUS_DUPES."
    )


def test_names_are_not_reassigned():
    """Одно имя — одно присваивание: второе перекрывает первое незаметно."""
    lines, _ = _int_assignments()
    redefined = {name for name, at in lines.items() if len(at) > 1}
    assert redefined == KNOWN_REDEFINED, (
        f"изменился набор дважды присвоенных имён.\nсейчас: {sorted(redefined)}\n"
        f"ожидалось: {sorted(KNOWN_REDEFINED)}\n"
        "Второе присваивание молча выигрывает — уберите лишнее."
    )
