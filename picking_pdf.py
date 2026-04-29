"""Лист сборки в PDF — байты для отправки в Telegram.

Адаптация nakladnye/picking_list.py: пишет в BytesIO, парсит состав
заказа из FIELD_COMPOSITION (поле 577313).
"""

import os
import re
from collections import Counter
from datetime import datetime
from io import BytesIO
from xml.sax.saxutils import escape
from zoneinfo import ZoneInfo

from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import HRFlowable, Paragraph, SimpleDocTemplate, Spacer

_FONT_CANDIDATES = [
    ("/System/Library/Fonts/Supplemental/Arial.ttf",
     "/System/Library/Fonts/Supplemental/Arial Bold.ttf"),
    ("/Library/Fonts/Arial.ttf",
     "/Library/Fonts/Arial Bold.ttf"),
    ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
     "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
]

_fonts_registered = False


def register_fonts() -> None:
    global _fonts_registered
    if _fonts_registered:
        return
    for reg, bold in _FONT_CANDIDATES:
        if os.path.exists(reg) and os.path.exists(bold):
            pdfmetrics.registerFont(TTFont("Body", reg))
            pdfmetrics.registerFont(TTFont("Body-Bold", bold))
            _fonts_registered = True
            return
    raise RuntimeError(
        "Не найден TTF с поддержкой кириллицы. "
        "Проверьте Arial или установите fonts-dejavu-core."
    )


_ITEM_RE = re.compile(
    r"(.+?),\s*(\d+)\s*шт,\s*[\d\s]+[.,]\d+\s*руб(?:ль|ля|лей|\.?)",
    re.IGNORECASE | re.DOTALL,
)


def parse_items(text: str | None) -> list[tuple[str, int]]:
    if not text:
        return []
    normalized = re.sub(r"\s+", " ", text)
    items = []
    for m in _ITEM_RE.finditer(normalized):
        name = m.group(1).strip(" ,;\n")
        count = int(m.group(2))
        if name:
            items.append((name, count))
    return items


def _html(text: str | None) -> str:
    return escape(text or "").replace("\n", "<br/>")


def build_pdf_bytes(leads_data: list[dict]) -> bytes:
    """leads_data: [{"contact_name", "cdek_number", "composition"}]."""
    register_fonts()

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=15 * mm, bottomMargin=15 * mm,
        title="Лист сборки",
    )

    gray = HexColor("#555555")
    line = HexColor("#cccccc")

    styles = {
        "title": ParagraphStyle(
            "title", fontName="Body-Bold", fontSize=13, leading=16, spaceAfter=6,
        ),
        "contact": ParagraphStyle(
            "contact", fontName="Body-Bold", fontSize=10, leading=12,
            spaceBefore=4, spaceAfter=1,
        ),
        "meta": ParagraphStyle(
            "meta", fontName="Body", fontSize=8, leading=10,
            textColor=gray, spaceAfter=1,
        ),
        "body": ParagraphStyle(
            "body", fontName="Body", fontSize=8, leading=10, spaceAfter=2,
        ),
        "sumhead": ParagraphStyle(
            "sumhead", fontName="Body-Bold", fontSize=11, leading=14,
            spaceBefore=6, spaceAfter=4,
        ),
        "sumrow": ParagraphStyle(
            "sumrow", fontName="Body", fontSize=9, leading=12,
        ),
    }

    today = datetime.now(ZoneInfo("Europe/Moscow")).strftime("%d.%m.%Y %H:%M")
    flowables = [
        Paragraph(f"Лист сборки — {today}", styles["title"]),
        Paragraph(f"Накладных: {len(leads_data)}", styles["meta"]),
        Spacer(1, 6),
    ]

    total_counter: Counter[str] = Counter()
    for lead in leads_data:
        flowables.append(Paragraph(_html(lead.get("contact_name") or "—"), styles["contact"]))
        flowables.append(Paragraph(f"Накладная: {_html(lead.get('cdek_number') or '—')}", styles["meta"]))
        flowables.append(Paragraph(_html(lead.get("composition")) or "—", styles["body"]))
        flowables.append(HRFlowable(
            width="100%", thickness=0.3, color=line,
            spaceBefore=4, spaceAfter=6,
        ))
        for name, count in parse_items(lead.get("composition")):
            total_counter[name] += count

    flowables.append(Paragraph("Итого к сборке", styles["sumhead"]))
    if total_counter:
        for name, count in sorted(total_counter.items(), key=lambda kv: (-kv[1], kv[0].lower())):
            flowables.append(Paragraph(f"{count} × {_html(name)}", styles["sumrow"]))
    else:
        flowables.append(Paragraph(
            "Не удалось распознать товары автоматически.",
            styles["body"],
        ))

    doc.build(flowables)
    return buf.getvalue()
