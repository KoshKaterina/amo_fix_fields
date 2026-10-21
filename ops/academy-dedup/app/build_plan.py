#!/usr/bin/env python3
"""Сборщик плана склейки дублей в воронке Академия (8642410). ТОЛЬКО ЧТЕНИЕ.

Что делает:
  1. читает все сделки воронки и их контакты;
  2. собирает людей объединением ключей (телефон / BotHelp / TelegramId) - транзитивно;
  3. в каждой группе выбирает выжившую и собирает тело merge для runner_template.js;
  4. пишет plan.json, report.txt и suspicious.json (группы, которые смотрим глазами).

Скрипт повторяемый: гонять можно сколько угодно раз, уже схлопнутые группы
исчезают сами (в воронке остаётся одна сделка на человека).

Запуск:  python3 build_plan.py [--out DIR]
"""
import sys, os, re, json, argparse, collections, datetime, random
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import amo

PIPELINE = 8642410
F_PHONE_CODE = "PHONE"
F_BOTHELP = 573753        # BotHelp_CUser_ID
F_TGID = 571839           # TelegramId_WZ
F_TGUSER = 577785         # TelegramUsername_WZ
STAGE_DO_NOT_TOUCH = 88485802   # «Не трогать этих клиентов!!!»
STAGE_APPLICATION = 88882518    # «Оставил заявку с Веба»
STAGE_ATTENDEE = 89189254       # «Присутствовали на вебе»
TAG_APPLICATION = "оставил заявку с веба"
TAG_ATTENDEE = "был на вебе 1"
# Склеиваем ТОЛЬКО внутри одного этапа (правило Кати 23.09.2026: «объединяем ТОЛЬКО
# в этом этапе», речь про «Входящий лид»). Сделку на любом другом этапе не трогаем
# вовсе - даже если она дубль того же человека. Снять ограничение: --any-stage.
ONLY_STAGE = 87654850           # Входящий лид
CLOSED = (142, 143)             # закрытые: история обращения, не дубль

# Свои карточки: тест-контакт и сотрудники. Их сделки в план не берём -
# там тестовые прогоны и личные пробы, склеивать нечего.
STOP_PHONES = {
    "9099371845",   # тест-контакт Кати
    "9259993699",   # Гладков Александер
}
STAGE_MS_ORDER = 576689         # ID заказа МойСклад на сделке
STAGE_PAY_LINK = 577617         # Ссылка для оплаты

# Ответственные Академии. Эти правила применяются ТОЛЬКО к группам склейки:
# остальные сделки воронки не переназначаются.
ARTEM = 13822630
ACADEMY_MANAGER_1 = 14289086   # Акчурина Нина / manager1@sunscrypt.ru
ACADEMY_MANAGER_2 = 14289446   # Калугин Денис / manager2@sunscrypt.ru
# Кирилл Полесский - в пуле Академии с 15.10, с 21.10 получает половину новых сделок из
# GetCourse. Без него в списке склейка его сделки бота с формой отдавала кому-то из троих.
ACADEMY_MANAGER_3 = 13946318   # Полесский Кирилл
ACADEMY_MANAGERS = (ACADEMY_MANAGER_1, ACADEMY_MANAGER_2, ACADEMY_MANAGER_3)
ACADEMY_TEAM = (ARTEM, *ACADEMY_MANAGERS)

# Ранг этапа повторяет фактическое положение слева направо в воронке:
# чем правее, тем выше приоритет. Единственное исключение — «Не трогать».
STAGE_RANK = {
    142: 20000,         # Успешно реализовано
    143: 19000,         # Закрыто и не реализовано
    # Спецэтап сохраняем при объединении: запрет контакта важнее прогресса.
    88485802: 18000,    # Не трогать этих клиентов!!!
    88464046: 250,      # Оплата получена
    88495938: 240,      # ссылка отправлена
    88464042: 230,      # Оплата запрошена
    88464038: 220,      # Рассрочка
    88528190: 210,      # Второе касание
    88527246: 200,      # Первичные материалы отправлены
    88466522: 190,      # Квалификация проведена
    88464034: 180,      # Взят в работу
    87654850: 170,      # Входящий лид
    88838390: 160,      # Записан на конференцию
    88882518: 150,      # Оставил заявку с Веба
    89189254: 140,      # Присутствовали на вебе
    88943006: 130,      # Вступил в чат
    88835666: 120,      # Записан на практикум
    88838386: 110,      # Анкета пройдена
    88838382: 100,      # Проходит анкету
    88838378: 90,       # Бот запущен
    89126550: 80,       # Ссылка на чат практикума отправлена
    89138194: 70,       # Контакт установлен
    89105518: 60,       # Не купили DeFi-3 анкета
    88943002: 50,       # Не купили DEFI-3
    88980210: 40,       # Не купили DeFi-3 с заявки
    70070966: 20,       # Лист ожидания
    70070962: 10,       # Неразобранное
}
STAGE_NAME = {
    142: "Успешно реализовано", 143: "Закрыто и не реализовано",
    88980210: "Не купили DeFi-3 с заявки", 88943002: "Не купили DEFI-3",
    89105518: "Не купили DeFi-3 анкета", 89138194: "Контакт установлен",
    89126550: "Ссылка на чат практикума отправлена", 88838378: "Бот запущен",
    88838382: "Проходит анкету", 88838386: "Анкета пройдена",
    88835666: "Записан на практикум", 88943006: "Вступил в чат",
    89189254: "Присутствовали на вебе", 88882518: "Оставил заявку с Веба",
    88838390: "Записан на конференцию", 88464046: "Оплата получена", 88495938: "ссылка отправлена",
    88464042: "Оплата запрошена", 88464038: "Рассрочка", 88528190: "Второе касание",
    88527246: "Первичные материалы отправлены", 88466522: "Квалификация проведена",
    88464034: "Взят в работу", 87654850: "Входящий лид",
    88485802: "Не трогать этих клиентов!!!", 70070966: "Лист ожидания",
    70070962: "Неразобранное",
}


def ts(t):
    return datetime.datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")


FNAMES, SNAPSHOT = {}, ""


# --- имя карточки и примечание о склейке ------------------------------------
# amo называет сделку «Сделка #12345», когда человек не дал ей имени. Такое имя
# не несёт смысла, и терять из-за него осмысленное имя поглощённой сделки нельзя
# (правило Кати 23.09.2026: имя вроде «ULTRA VIP первому позвонить» оставляем).
AUTO_NAME = re.compile(r"^\s*(сделка|deal)\s*#?\s*\d*\s*$", re.I)


def is_auto_name(n):
    return not n or not n.strip() or bool(AUTO_NAME.match(n))


def field_names():
    """{id поля: человеческое название} - чтобы примечание читал человек, а не робот."""
    out = {}
    r = amo.get("leads/custom_fields?limit=250") or {}
    if r.get("_error"):
        raise RuntimeError(f"amo leads/custom_fields failed: {r.get('_error')}")
    for f in ((r.get("_embedded") or {}).get("custom_fields") or []):
        out[f["id"]] = f.get("name") or str(f["id"])
    return out


def build_note(survivor, absorbed, conflicts, name_from, extra_names, price_lost, fnames, snapshot):
    """Текст примечания на выжившую: что поглощено и что не поместилось в поля.

    Пишем ВСЕГДА, даже когда терять нечего: события истории поглощённой сделки
    умирают при склейке, и без примечания от неё не остаётся вообще ничего.
    """
    today = datetime.date.today().strftime("%d.%m.%Y")
    lines = [f"Склейка дублей воронки Академия, {today}. Поглощено сделок: {len(absorbed)}."]

    lines.append("")
    lines.append("Поглощены:")
    for a in absorbed[:20]:
        nm = a.get("name") or "без имени"
        lines.append(f"• «{nm}», создана {ts(a['created_at'])}, этап «{STAGE_NAME.get(a['status_id'], a['status_id'])}»")
    if len(absorbed) > 20:
        lines.append(f"• и ещё {len(absorbed) - 20}")

    if conflicts:
        lines.append("")
        lines.append("Значения, которые не попали в карточку (поле уже было заполнено иначе):")
        for fid, kept, lost in conflicts:
            lines.append(f"• {fnames.get(fid, fid)}: не поместилось «{lost}», осталось «{kept}»")

    if name_from:
        lines.append("")
        lines.append(f"Имя карточки взято из поглощённой сделки: «{name_from}».")
    if extra_names:
        lines.append("Другие имена поглощённых сделок: " + ", ".join(f"«{n}»" for n in extra_names) + ".")

    if price_lost:
        lines.append("")
        lines.append("Бюджет поглощённых сделок: " + ", ".join(str(v) for v in price_lost) + ".")

    lines.append("")
    lines.append(f"Все поля всех сделок до склейки лежат в снимке {snapshot}.")
    return "\n".join(lines)


def pull_leads():
    # Листаем по id и отсекаем повторы: сделка, изменённая посреди чтения, иначе
    # попадает на две страницы, и сторож пытается склеить её саму с собой
    # (15.10.2026, сделка 36575937, amo ответила Error 332).
    out, seen, page = [], set(), 1
    while True:
        r = amo.get(f"leads?filter[pipeline_id]={PIPELINE}&with=contacts&order[id]=asc&limit=250&page={page}")
        if not isinstance(r, dict) or r.get("_error"):
            code = r.get("_error") if isinstance(r, dict) else "invalid_response"
            raise RuntimeError(f"amo leads page {page} failed: {code}")
        chunk = ((r or {}).get("_embedded") or {}).get("leads") or []
        if not chunk:
            break
        for lead in chunk:
            if lead["id"] not in seen:
                seen.add(lead["id"])
                out.append(lead)
        page += 1
        if page > 80:
            break
    return out


def pull_contacts(ids):
    out, ids = {}, sorted(ids)
    for i in range(0, len(ids), 100):
        q = "&".join(f"filter[id][]={x}" for x in ids[i:i + 100])
        r = amo.get(f"contacts?{q}&limit=250")
        if not isinstance(r, dict) or r.get("_error"):
            code = r.get("_error") if isinstance(r, dict) else "invalid_response"
            raise RuntimeError(f"amo contacts batch {i // 100 + 1} failed: {code}")
        for c in ((r or {}).get("_embedded") or {}).get("contacts") or []:
            out[c["id"]] = c
    return out


def cf(entity, field_id):
    for fv in (entity.get("custom_fields_values") or []):
        if fv.get("field_id") == field_id and fv.get("values"):
            return fv["values"][0].get("value")
    return None


def cf_all(entity):
    """Все заполненные кастом-поля сделки: {field_id: значение или список}."""
    out = {}
    for fv in (entity.get("custom_fields_values") or []):
        vals = [v.get("value") for v in fv.get("values") or [] if v.get("value") not in (None, "")]
        if not vals:
            continue
        out[fv["field_id"]] = vals[0] if len(vals) == 1 else vals
    return out


def phone(c):
    for fv in (c.get("custom_fields_values") or []):
        if fv.get("field_code") == F_PHONE_CODE and fv.get("values"):
            d = "".join(ch for ch in str(fv["values"][0].get("value")) if ch.isdigit())
            if len(d) >= 10:
                return d[-10:]
    return None


def contact_keys(c):
    """Ключи, по которым карточка признаётся тем же человеком. Имя НЕ ключ - тёзки."""
    keys = []
    p = phone(c)
    if p:
        keys.append("ph:" + p)
    for fid, pref in ((F_BOTHELP, "bh"), (F_TGID, "tg"), (F_TGUSER, "un")):
        v = cf(c, fid)
        if v not in (None, ""):
            keys.append(f"{pref}:{str(v).strip().lower().lstrip("@")}")
    return keys


def contact_weight(c):
    """Насколько карточка «настоящая»: по ней выбираем главный контакт группы."""
    w = 0
    if phone(c):
        w += 5
    for fv in (c.get("custom_fields_values") or []):
        if fv.get("field_code") == "EMAIL":
            w += 3
    if cf(c, 577569):           # Общая сумма покупок
        w += 4
    if cf(c, F_TGID):
        w += 1
    if c.get("created_by"):     # создан человеком, а не интеграцией
        w += 2
    name = (c.get("name") or "").strip()
    if len(name.split()) > 1:   # «Имя Фамилия» весомее ника
        w += 2
    return w


def lead_has_human_work(lead):
    """Признаки, что сделкой занимались руками, а не только бот её создал."""
    marks = 0
    name = (lead.get("name") or "")
    if name and not name.startswith("Сделка #"):
        marks += 1
    if lead.get("price"):
        marks += 1
    if lead.get("created_by"):
        marks += 1
    fields = cf_all(lead)
    # UTM ставит бот, они в счёт не идут
    meaningful = {k for k in fields if k not in (559303, 559305, 559307, 559311, 559309)}
    marks += len(meaningful)
    return marks


def union(groups_keys):
    """Простой union-find по ключам контактов."""
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def unite(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for keys in groups_keys:
        for k in keys[1:]:
            unite(keys[0], k)
    return find


def choose_responsible(group, person_key):
    """Выбрать ответственного только для результата склейки по правилу Кати 08.10.2026."""
    owners = sorted({int(l["responsible_user_id"]) for l in group if l.get("responsible_user_id")})
    if ARTEM in owners:
        return ARTEM, "в группе есть Артём"

    academy_present = [uid for uid in ACADEMY_MANAGERS if uid in owners]
    chooser = random.Random(f"academy-dedup-2026-10-08:{person_key}")
    if len(academy_present) == 1:
        return academy_present[0], "сохранён единственный менеджер Академии из группы"
    if len(academy_present) > 1:
        return chooser.choice(academy_present), "случайно выбран один из менеджеров Академии в группе"

    return chooser.choice(ACADEMY_TEAM), "прочий ответственный перераспределён внутри команды Академии"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.dirname(os.path.abspath(__file__)))
    ap.add_argument("--any-stage", action="store_true",
                    help="склеивать сделки на разных этапах (по умолчанию только «Входящий лид»)")
    ap.add_argument("--merge-closed", action="store_true",
                    help="поглощать и закрытые сделки (по умолчанию закрытые не трогаем)")
    ap.add_argument("--prefer-other-stage", action="store_true",
                    help="выживает сделка ЛЮБОГО другого этапа, «Входящий лид» поглощается "
                         "всегда (правило Кати 23.09: «склей в пользу сделки из другого этапа»)")
    args = ap.parse_args()

    # «Входящий лид» опускаем ниже всех прочих открытых этапов: по правилу Кати
    # выживает сделка, которая уже ушла из входящего, каким бы этот этап ни был -
    # хоть «Лист ожидания», хоть «Не трогать этих клиентов!!!».
    if args.prefer_other_stage:
        STAGE_RANK[ONLY_STAGE] = 1

    # для примечаний: человеческие названия полей и имя последнего снимка воронки
    global FNAMES, SNAPSHOT
    FNAMES = field_names()
    bdir = Path(os.path.dirname(os.path.abspath(__file__))) / "backups"
    snaps = sorted(d.name for d in bdir.iterdir() if d.is_dir()) if bdir.exists() else []
    SNAPSHOT = str(Path(args.out))

    leads = pull_leads()
    cids = {c["id"] for l in leads for c in ((l.get("_embedded") or {}).get("contacts") or [])}
    contacts = pull_contacts(cids)

    # 1. Союз ключей: один человек = связанные между собой ключи разных карточек
    key_sets = [contact_keys(c) for c in contacts.values() if contact_keys(c)]
    find = union(key_sets)

    def person_of(lead):
        keys = []
        for c in ((lead.get("_embedded") or {}).get("contacts") or []):
            cc = contacts.get(c["id"])
            if cc:
                keys += contact_keys(cc)
        if keys:
            return find(keys[0])
        cs = (lead.get("_embedded") or {}).get("contacts") or []
        return f"cid:{cs[0]['id']}" if cs else f"lead:{lead['id']}"

    by_person = collections.defaultdict(list)
    for l in leads:
        by_person[person_of(l)].append(l)

    plan, suspicious, skipped = [], [], []
    for pkey, group in sorted(by_person.items()):
        # свои карточки мимо: тест и сотрудники
        own = False
        for l in group:
            for c in ((l.get("_embedded") or {}).get("contacts") or []):
                cc = contacts.get(c["id"])
                if cc and phone(cc) in STOP_PHONES:
                    own = True
        if own:
            if len(group) > 1:
                skipped.append({"person": pkey, "leads": [l["id"] for l in group],
                                "why": "своя карточка (тест или сотрудник)"})
            continue

        # закрытая сделка - история прошлого обращения, а не дубль
        if not args.merge_closed:
            closed = [l for l in group if l["status_id"] in CLOSED]
            group = [l for l in group if l["status_id"] not in CLOSED]
            if closed and len(group) < 2:
                continue
        # только один этап: сделки человека на других этапах остаются как есть
        if ONLY_STAGE and not args.any_stage:
            off = [l for l in group if l["status_id"] != ONLY_STAGE]
            group = [l for l in group if l["status_id"] == ONLY_STAGE]
            if off and len(group) < 2:
                continue
        if len(group) < 2:
            continue
        # выжившая: дальше по воронке → больше ручной работы → раньше создана
        ranked = sorted(
            group,
            key=lambda l: (STAGE_RANK.get(l["status_id"], 0), lead_has_human_work(l), -l["created_at"]),
            reverse=True,
        )
        survivor, absorbed = ranked[0], ranked[1:]
        result_responsible, responsible_rule = choose_responsible(group, pkey)

        # поля: значение выжившей главное, пустые добираем с поглощаемых (сохраняем максимум).
        # Заполнено у обеих по-разному - значение выжившей остаётся в поле, второе уходит
        # в примечание. В поле оба не пишем: там utm и прочая разметка, склейка «open, closed»
        # ломает группировку в аналитике, а примечание не ломает ничего.
        cfv = dict(cf_all(survivor))
        conflicts, seen_lost = [], set()
        for a in absorbed:
            for fid, val in cf_all(a).items():
                if fid not in cfv:
                    cfv[fid] = val
                elif str(cfv[fid]) != str(val) and (fid, str(val)) not in seen_lost:
                    seen_lost.add((fid, str(val)))
                    conflicts.append((fid, cfv[fid], val))

        # имя: автогенерённое «Сделка #N» не держим, берём первое осмысленное из группы
        name = (survivor.get("name") or "").strip()
        name_from, extra_names = None, []
        for a in absorbed:
            an = (a.get("name") or "").strip()
            if is_auto_name(an):
                continue
            if is_auto_name(name):
                name, name_from = an, an
            elif an != name and an not in extra_names:
                extra_names.append(an)

        # бюджет поглощённых: в поле не переносим (сумма склеилась бы неверно), пишем в примечание
        price_lost = [a.get("price") for a in absorbed if a.get("price")]

        # контакты: оставляем ВСЕ карточки группы, склейка контактов - отдельный шаг
        gcontacts, main_contact, best_w = [], None, -1
        for l in group:
            for c in ((l.get("_embedded") or {}).get("contacts") or []):
                if c["id"] not in gcontacts:
                    gcontacts.append(c["id"])
                cc = contacts.get(c["id"])
                if cc and contact_weight(cc) > best_w:
                    best_w, main_contact = contact_weight(cc), c["id"]

        item = {
            "person": pkey,
            "survivor": survivor["id"],
            "survivor_status_id": survivor["status_id"],
            "survivor_stage": STAGE_NAME.get(survivor["status_id"], survivor["status_id"]),
            "absorbed": [a["id"] for a in absorbed],
            "main_contact": main_contact,
            "contacts_in_group": gcontacts,
            "result_element": {
                "ID": survivor["id"],
                "NAME": name,
                "MAIN_USER_ID": result_responsible,
                "DATE_CREATE": ts(survivor["created_at"]),
                "PRICE": survivor.get("price") or 0,
                "cfv": {str(k): v for k, v in cfv.items()},
                "CONTACTS": gcontacts,
            },
            "note": build_note(survivor, absorbed, conflicts, name_from,
                               extra_names, price_lost, FNAMES, SNAPSHOT),
            "responsible_before": sorted({
                int(l["responsible_user_id"])
                for l in group
                if l.get("responsible_user_id")
            }),
            "responsible_after": result_responsible,
            "responsible_rule": responsible_rule,
            "needs_application_followup": any(l["status_id"] == STAGE_APPLICATION for l in group),
            "needs_attendee_tag": any(l["status_id"] == STAGE_ATTENDEE for l in group),
            "tags_after": sorted({
                tag.get("name")
                for lead in group
                for tag in ((lead.get("_embedded") or {}).get("tags") or [])
                if tag.get("name")
            } | ({TAG_APPLICATION} if any(l["status_id"] == STAGE_APPLICATION for l in group) else set())
              | ({TAG_ATTENDEE} if any(l["status_id"] == STAGE_ATTENDEE for l in group) else set())),
        }

        # что смотрим глазами, а не гоним автоматом
        why = []
        if any(l["status_id"] == STAGE_DO_NOT_TOUCH for l in group):
            why.append("есть сделка на этапе «Не трогать этих клиентов!!!»")
        if any(cf(l, STAGE_MS_ORDER) for l in group):
            why.append("есть заказ МойСклада")
        if any(cf(l, STAGE_PAY_LINK) for l in group):
            why.append("есть ссылка на оплату")
        if len({l["status_id"] for l in group}) > 1:
            why.append("сделки на разных этапах")
        if any(l.get("price") for l in group):
            why.append("ненулевой бюджет")
        # несколько карточек контакта - штатная картина этого потока, не повод для рук

        item["flags"] = why
        (suspicious if why else plan).append(item)

    os.makedirs(args.out, exist_ok=True)
    json.dump(plan, open(f"{args.out}/plan.json", "w"), ensure_ascii=False, indent=1)
    json.dump(suspicious, open(f"{args.out}/suspicious.json", "w"), ensure_ascii=False, indent=1)
    json.dump(skipped, open(f"{args.out}/skipped.json", "w"), ensure_ascii=False, indent=1)

    total_absorbed = sum(len(i["absorbed"]) for i in plan)
    susp_absorbed = sum(len(i["absorbed"]) for i in suspicious)
    lines = [
        f"Собрано {datetime.datetime.now():%Y-%m-%d %H:%M}",
        f"сделок в воронке: {len(leads)}",
        f"людей: {len(by_person)}",
        f"групп с дублями: {len(plan) + len(suspicious)}",
        f"  чистых (гоним автоматом): {len(plan)}, под удаление {total_absorbed}",
        f"  под глаза: {len(suspicious)}, под удаление {susp_absorbed}",
        f"итого воронка ужмётся: {len(leads)} -> {len(leads) - total_absorbed - susp_absorbed}",
        f"пропущено своих групп (тест и сотрудники): {len(skipped)}",
        ("закрытые сделки ПОГЛОЩАЮТСЯ" if args.merge_closed
         else "закрытые сделки не трогаем (история обращения)"),
        f"групп, где у человека больше одной карточки контакта: "
        f"{sum(1 for i in plan + suspicious if len(i['contacts_in_group']) > 1)}"
        f" (лишних карточек {sum(len(i['contacts_in_group']) - 1 for i in plan + suspicious)})",
        "",
        "причины отправки под глаза:",
    ]
    reasons = collections.Counter(w for i in suspicious for w in i["flags"])
    for w, n in reasons.most_common():
        lines.append(f"  {n:4d}  {w}")
    open(f"{args.out}/report.txt", "w").write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
