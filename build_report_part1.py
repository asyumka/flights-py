"""
Генератор Пояснювальної записки курсової роботи у форматі DOCX згідно з методичними
рекомендаціями НУ «Київський авіаційний інститут» / «Київський національний інститут» (2026 р.).

Виконала: здобувачка групи M-F3-26-1-ВД Діана КИЯН
Варіант №3: «Аналіз пунктуальності авіарейсів і чинників затримок»
Повна назва теми: «Конвеєр обробки та аналізу великих даних пунктуальності авіарейсів і чинників затримок»
Програмно-аналітичний комплекс: «ПАКАПА»
"""
from __future__ import annotations
from pathlib import Path
from typing import List, Sequence

import docx
from docx import Document
from docx.enum.section import WD_SECTION
from docx.enum.table import WD_ALIGN_VERTICAL, WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import nsdecls, qn
from docx.shared import Cm, Mm, Pt, RGBColor

ROOT_DIR = Path("d:/ed/course/flights")
FIG_DIR = ROOT_DIR / "docs" / "figures"
OUT_DOCX_MAIN = ROOT_DIR / "Пояснювальна_записка_КР_Киян_Діана_M-F3-26-1-ВД_Варіант_3.docx"
OUT_DOCX_DOCS = ROOT_DIR / "docs" / "Курсова_робота_Киян_Діана_M-F3-26-1-ВД.docx"


def set_cell_margins(cell, top=80, bottom=80, left=110, right=110):
    tcPr = cell._tc.get_or_add_tcPr()
    tcMar = OxmlElement("w:tcMar")
    for m, val in [("top", top), ("bottom", bottom), ("left", left), ("right", right)]:
        node = OxmlElement(f"w:{m}")
        node.set(qn("w:w"), str(val))
        node.set(qn("w:type"), "dxa")
        tcMar.append(node)
    tcPr.append(tcMar)


def set_cell_shading(cell, color_hex: str):
    shading = parse_xml(f'<w:shd {nsdecls("w")} w:fill="{color_hex}"/>')
    cell._tc.get_or_add_tcPr().append(shading)


def add_page_number_footer(section):
    """Додає наскрізну нумерацію сторінок арабськими цифрами в правому нижньому куту (без 1 на титулці)."""
    section.different_first_page_header_footer = True
    footer = section.footer
    p = footer.paragraphs[0]
    p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    p.paragraph_format.first_line_indent = Cm(0)
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(0)

    run = p.add_run()
    run.font.name = "Times New Roman"
    run.font.size = Pt(12)

    fldChar1 = OxmlElement("w:fldChar")
    fldChar1.set(qn("w:fldCharType"), "begin")
    instrText = OxmlElement("w:instrText")
    instrText.set(qn("xml:space"), "preserve")
    instrText.text = "PAGE"
    fldChar2 = OxmlElement("w:fldChar")
    fldChar2.set(qn("w:fldCharType"), "separate")
    fldChar3 = OxmlElement("w:fldChar")
    fldChar3.set(qn("w:fldCharType"), "end")

    r = run._r
    r.append(fldChar1)
    r.append(instrText)
    r.append(fldChar2)
    r.append(fldChar3)


def format_run(run, size_pt=14, bold=False, italic=False, font_name="Times New Roman", color_rgb=None):
    run.font.name = font_name
    run._element.rPr.rFonts.set(qn("w:eastAsia"), font_name)
    run.font.size = Pt(size_pt)
    run.bold = bold
    run.italic = italic
    if color_rgb:
        run.font.color.rgb = RGBColor(*color_rgb)


def add_body_p(doc: Document, text: str, bold_prefix: str = "", indent_cm: float = 1.25, align=WD_ALIGN_PARAGRAPH.JUSTIFY):
    p = doc.add_paragraph()
    p.alignment = align
    pf = p.paragraph_format
    pf.first_line_indent = Cm(indent_cm)
    pf.line_spacing_rule = WD_LINE_SPACING.ONE_POINT_FIVE
    pf.space_before = Pt(0)
    pf.space_after = Pt(0)

    if bold_prefix:
        r_pre = p.add_run(bold_prefix)
        format_run(r_pre, 14, bold=True)
    r_main = p.add_run(text)
    format_run(r_main, 14, bold=False)
    return p


def add_bullet_p(doc: Document, text: str, bold_prefix: str = ""):
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    pf = p.paragraph_format
    pf.first_line_indent = Cm(1.25)
    pf.line_spacing_rule = WD_LINE_SPACING.ONE_POINT_FIVE
    pf.space_before = Pt(0)
    pf.space_after = Pt(0)

    r_dash = p.add_run("− ")
    format_run(r_dash, 14, bold=False)
    if bold_prefix:
        r_b = p.add_run(bold_prefix)
        format_run(r_b, 14, bold=True)
    r_t = p.add_run(text)
    format_run(r_t, 14, bold=False)
    return p


def add_chapter_heading(doc: Document, text: str, new_page: bool = True):
    """Заголовки розділів пишуть великими літерами посередині тексту без крапки в кінці."""
    if new_page:
        doc.add_page_break()
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    pf = p.paragraph_format
    pf.first_line_indent = Cm(0)
    pf.line_spacing_rule = WD_LINE_SPACING.ONE_POINT_FIVE
    pf.space_before = Pt(12)
    pf.space_after = Pt(18)
    pf.keep_with_next = True
    r = p.add_run(text.upper().rstrip("."))
    format_run(r, 14, bold=True)
    return p


def add_sub_heading(doc: Document, text: str):
    """Заголовки підрозділів пишуть з абзацу (1,25 см), малими літерами крім першої великої, без крапки."""
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.LEFT
    pf = p.paragraph_format
    pf.first_line_indent = Cm(1.25)
    pf.line_spacing_rule = WD_LINE_SPACING.ONE_POINT_FIVE
    pf.space_before = Pt(14)
    pf.space_after = Pt(10)
    pf.keep_with_next = True
    r = p.add_run(text.rstrip("."))
    format_run(r, 14, bold=True)
    return p


def add_formula(doc: Document, formula_str: str, number_str: str, explanations: Sequence[str]):
    """
    Оформлення формул за розділом 7 методички:
    - Виділяються в окремий рядок по середині, інтервал 12 пт вище і нижче, напівжирний 14 пт,
      номер справа у дужках (наприклад (1.1)).
    - Після формули кома, наступний рядок без абзацу починається зі слова «де» без двокрапки.
    """
    tbl = doc.add_table(rows=1, cols=2)
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    tbl.autofit = False

    # Прибираємо рамки таблиці-контейнера формули
    tblPr = tbl._tbl.tblPr
    tblBorders = parse_xml(
        f'<w:tblBorders {nsdecls("w")}>'
        '<w:top w:val="none" w:sz="0" w:space="0" w:color="auto"/>'
        '<w:left w:val="none" w:sz="0" w:space="0" w:color="auto"/>'
        '<w:bottom w:val="none" w:sz="0" w:space="0" w:color="auto"/>'
        '<w:right w:val="none" w:sz="0" w:space="0" w:color="auto"/>'
        '<w:insideH w:val="none" w:sz="0" w:space="0" w:color="auto"/>'
        '<w:insideV w:val="none" w:sz="0" w:space="0" w:color="auto"/>'
        "</w:tblBorders>"
    )
    tblPr.append(tblBorders)

    c0 = tbl.cell(0, 0)
    c1 = tbl.cell(0, 1)
    c0.width = Cm(15.2)
    c1.width = Cm(2.3)

    p0 = c0.paragraphs[0]
    p0.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p0.paragraph_format.first_line_indent = Cm(0)
    p0.paragraph_format.space_before = Pt(12)
    p0.paragraph_format.space_after = Pt(12)
    r0 = p0.add_run(f"{formula_str},")
    format_run(r0, 14, bold=True, italic=True)

    p1 = c1.paragraphs[0]
    p1.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    p1.paragraph_format.first_line_indent = Cm(0)
    p1.paragraph_format.space_before = Pt(12)
    p1.paragraph_format.space_after = Pt(12)
    r1 = p1.add_run(f"({number_str})")
    format_run(r1, 14, bold=False)

    for idx, exp in enumerate(explanations):
        pe = doc.add_paragraph()
        pe.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
        pe.paragraph_format.first_line_indent = Cm(0 if idx == 0 else 1.25)
        pe.paragraph_format.line_spacing_rule = WD_LINE_SPACING.ONE_POINT_FIVE
        pe.paragraph_format.space_before = Pt(0)
        pe.paragraph_format.space_after = Pt(0)
        prefix = f"де {exp}" if idx == 0 else exp
        re = pe.add_run(prefix)
        format_run(re, 14, bold=False)


def add_figure(doc: Document, img_path: Path, fig_num: str, caption: str, width_cm: float = 16.2):
    """
    Оформлення рисунків за розділом 7 методички:
    - Від основного тексту інтервал 12 пт, вирівнювання по середині рядка.
    - Підпис під зображенням по середині: «Рис. Х.У. Назва ілюстрації» без крапки наприкінці.
    """
    if not img_path.exists():
        return
    p_img = doc.add_paragraph()
    p_img.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_img.paragraph_format.first_line_indent = Cm(0)
    p_img.paragraph_format.space_before = Pt(12)
    p_img.paragraph_format.space_after = Pt(6)
    p_img.paragraph_format.keep_with_next = True
    run_img = p_img.add_run()
    run_img.add_picture(str(img_path), width=Cm(width_cm))

    p_cap = doc.add_paragraph()
    p_cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_cap.paragraph_format.first_line_indent = Cm(0)
    p_cap.paragraph_format.space_before = Pt(2)
    p_cap.paragraph_format.space_after = Pt(12)
    r_cap = p_cap.add_run(f"Рис. {fig_num}. {caption.rstrip('.')}")
    format_run(r_cap, 14, bold=False)


def add_styled_table(
    doc: Document,
    table_num: str,
    title: str,
    headers: Sequence[str],
    rows: Sequence[Sequence[str]],
    col_widths_cm: Sequence[float] | None = None,
    font_size: int = 12,
):
    """
    Оформлення таблиць за розділом 7 методички:
    - Слово «Таблиця Х.У» праворуч над назвою.
    - Назва таблиці центрується відносно таблиці без крапки в кінці.
    - Відокремлена від основного тексту інтервалом 12 пт.
    - Текст у таблиці розміром 12 пт.
    """
    p_num = doc.add_paragraph()
    p_num.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    p_num.paragraph_format.first_line_indent = Cm(0)
    p_num.paragraph_format.space_before = Pt(12)
    p_num.paragraph_format.space_after = Pt(2)
    p_num.paragraph_format.keep_with_next = True
    r_num = p_num.add_run(f"Таблиця {table_num}")
    format_run(r_num, 14, bold=False)

    p_title = doc.add_paragraph()
    p_title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_title.paragraph_format.first_line_indent = Cm(0)
    p_title.paragraph_format.space_before = Pt(0)
    p_title.paragraph_format.space_after = Pt(6)
    p_title.paragraph_format.keep_with_next = True
    r_title = p_title.add_run(title.rstrip("."))
    format_run(r_title, 14, bold=True)

    tbl = doc.add_table(rows=1 + len(rows), cols=len(headers))
    tbl.style = "Table Grid"
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER

    hdr_cells = tbl.rows[0].cells
    for j, h_text in enumerate(headers):
        hdr_cells[j].vertical_alignment = WD_ALIGN_VERTICAL.CENTER
        set_cell_margins(hdr_cells[j], 70, 70, 90, 90)
        set_cell_shading(hdr_cells[j], "E8EEF5")
        ph = hdr_cells[j].paragraphs[0]
        ph.alignment = WD_ALIGN_PARAGRAPH.CENTER
        ph.paragraph_format.first_line_indent = Cm(0)
        ph.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE
        ph.paragraph_format.space_before = Pt(2)
        ph.paragraph_format.space_after = Pt(2)
        rh = ph.add_run(h_text.rstrip("."))
        format_run(rh, font_size, bold=True)

    for i, row_data in enumerate(rows):
        row_cells = tbl.rows[i + 1].cells
        for j, val in enumerate(row_data):
            row_cells[j].vertical_alignment = WD_ALIGN_VERTICAL.CENTER
            set_cell_margins(row_cells[j], 55, 55, 85, 85)
            pc = row_cells[j].paragraphs[0]
            pc.paragraph_format.first_line_indent = Cm(0)
            pc.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE
            pc.paragraph_format.space_before = Pt(1)
            pc.paragraph_format.space_after = Pt(1)
            val_str = str(val).rstrip(".") if j == 0 else str(val)
            # Вирівнювання числових даних за правим краєм або по центру, текстових — ліворуч
            if j > 0 and any(ch.isdigit() for ch in val_str) and len(val_str) <= 18:
                pc.alignment = WD_ALIGN_PARAGRAPH.CENTER
            else:
                pc.alignment = WD_ALIGN_PARAGRAPH.LEFT
            rc = pc.add_run(val_str)
            format_run(rc, font_size, bold=False)

    if col_widths_cm:
        for row in tbl.rows:
            for j, w_cm in enumerate(col_widths_cm):
                row.cells[j].width = Cm(w_cm)

    p_space = doc.add_paragraph()
    p_space.paragraph_format.space_before = Pt(0)
    p_space.paragraph_format.space_after = Pt(6)


def add_code_block(doc: Document, title: str, code_text: str):
    p_cap = doc.add_paragraph()
    p_cap.alignment = WD_ALIGN_PARAGRAPH.LEFT
    p_cap.paragraph_format.first_line_indent = Cm(0)
    p_cap.paragraph_format.space_before = Pt(8)
    p_cap.paragraph_format.space_after = Pt(3)
    p_cap.paragraph_format.keep_with_next = True
    r_cap = p_cap.add_run(title)
    format_run(r_cap, 12, bold=True)

    tbl = doc.add_table(rows=1, cols=1)
    tbl.style = "Table Grid"
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    cell = tbl.cell(0, 0)
    set_cell_shading(cell, "F8FAFC")
    set_cell_margins(cell, 80, 80, 120, 120)
    p = cell.paragraphs[0]
    p.alignment = WD_ALIGN_PARAGRAPH.LEFT
    p.paragraph_format.first_line_indent = Cm(0)
    p.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(0)
    r = p.add_run(code_text.strip("\n"))
    format_run(r, 9.5, bold=False, font_name="Consolas")


def build_front_matter(doc: Document):
    """Формує Титульний аркуш (Додаток А), Завдання (Додаток Б), Реферат (Додаток В) та Зміст."""
    # -------------------------------------------------------------------------
    # 1. ТИТУЛЬНИЙ АРКУШ (за зразком Додатка А методички, стор. 23-24)
    # -------------------------------------------------------------------------
    header_lines = [
        "МІНІСТЕРСТВО ОСВІТИ І НАУКИ УКРАЇНИ",
        "НАЦІОНАЛЬНИЙ УНІВЕРСИТЕТ",
        '"КИЇВСЬКИЙ НАЦІОНАЛЬНИЙ ІНСТИТУТ"',
        "Факультет комп’ютерних наук та технологій",
        "Кафедра комп’ютерних інформаційних технологій",
    ]
    for idx, line in enumerate(header_lines):
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        p.paragraph_format.first_line_indent = Cm(0)
        p.paragraph_format.line_spacing_rule = WD_LINE_SPACING.ONE_POINT_FIVE
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after = Pt(2 if idx < 4 else 42)
        r = p.add_run(line)
        format_run(r, 14, bold=True)

    p_kr = doc.add_paragraph()
    p_kr.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_kr.paragraph_format.first_line_indent = Cm(0)
    p_kr.paragraph_format.space_before = Pt(28)
    p_kr.paragraph_format.space_after = Pt(12)
    r_kr = p_kr.add_run("Курсова робота")
    format_run(r_kr, 20, bold=True)

    p_disc = doc.add_paragraph()
    p_disc.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_disc.paragraph_format.first_line_indent = Cm(0)
    p_disc.paragraph_format.line_spacing_rule = WD_LINE_SPACING.ONE_POINT_FIVE
    p_disc.paragraph_format.space_after = Pt(14)
    r_disc = p_disc.add_run(
        "з дисципліни «Технології й інфраструктура великих даних для аналізу даних»\nна тему:"
    )
    format_run(r_disc, 14, bold=False)

    p_topic = doc.add_paragraph()
    p_topic.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_topic.paragraph_format.first_line_indent = Cm(0)
    p_topic.paragraph_format.line_spacing_rule = WD_LINE_SPACING.ONE_POINT_FIVE
    p_topic.paragraph_format.space_after = Pt(54)
    r_topic = p_topic.add_run(
        "«Конвеєр обробки та аналізу великих даних пунктуальності авіарейсів і чинників затримок»\n(Варіант № 3)"
    )
    format_run(r_topic, 15, bold=True)

    # Блок підписів на титульному аркуші
    sig_tbl = doc.add_table(rows=2, cols=2)
    sig_tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    sig_Borders = parse_xml(
        f'<w:tblBorders {nsdecls("w")}>'
        '<w:top w:val="none" w:sz="0" w:space="0" w:color="auto"/>'
        '<w:left w:val="none" w:sz="0" w:space="0" w:color="auto"/>'
        '<w:bottom w:val="none" w:sz="0" w:space="0" w:color="auto"/>'
        '<w:right w:val="none" w:sz="0" w:space="0" w:color="auto"/>'
        '<w:insideH w:val="none" w:sz="0" w:space="0" w:color="auto"/>'
        '<w:insideV w:val="none" w:sz="0" w:space="0" w:color="auto"/>'
        "</w:tblBorders>"
    )
    sig_tbl._tbl.tblPr.append(sig_Borders)

    sig_data = [
        ("Виконала:", "здобувачка групи M-F3-26-1-ВД\nДіана КИЯН"),
        ("Перевірив:", "канд. техн. наук, доц. кафедри КІТ\nКостянтин ПРОКОПЕНКО"),
    ]
    for i, (left_txt, right_txt) in enumerate(sig_data):
        c_left = sig_tbl.cell(i, 0)
        c_right = sig_tbl.cell(i, 1)
        c_left.width = Cm(8.0)
        c_right.width = Cm(9.5)
        pl = c_left.paragraphs[0]
        pl.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        pl.paragraph_format.first_line_indent = Cm(0)
        pl.paragraph_format.space_after = Pt(18)
        rl = pl.add_run(left_txt + "    ")
        format_run(rl, 14, bold=False)

        pr = c_right.paragraphs[0]
        pr.alignment = WD_ALIGN_PARAGRAPH.LEFT
        pr.paragraph_format.first_line_indent = Cm(0)
        pr.paragraph_format.line_spacing_rule = WD_LINE_SPACING.ONE_POINT_FIVE
        pr.paragraph_format.space_after = Pt(18)
        rr = pr.add_run(right_txt)
        format_run(rr, 14, bold=False)

    p_city = doc.add_paragraph()
    p_city.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_city.paragraph_format.first_line_indent = Cm(0)
    p_city.paragraph_format.space_before = Pt(72)
    r_city = p_city.add_run("Київ 2026")
    format_run(r_city, 14, bold=False)

    # -------------------------------------------------------------------------
    # 2. БЛАНК ЗАВДАННЯ НА КУРСОВУ РОБОТУ (Додаток Б методички, стор. 24-26)
    # -------------------------------------------------------------------------
    doc.add_page_break()
    for line in ["НАЦІОНАЛЬНИЙ УНІВЕРСИТЕТ", '"КИЇВСЬКИЙ НАЦІОНАЛЬНИЙ ІНСТИТУТ"']:
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        p.paragraph_format.first_line_indent = Cm(0)
        p.paragraph_format.space_after = Pt(2)
        r = p.add_run(line)
        format_run(r, 14, bold=True)

    meta_items = [
        ("Факультет:", " Комп’ютерних наук та технологій"),
        ("Кафедра:", " Комп’ютерних інформаційних технологій"),
        ("Спеціальність:", " F3 «Комп’ютерні науки»"),
        ("Освітньо-професійна програма:", " «Технології обробки великих даних»"),
    ]
    for k, v in meta_items:
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.LEFT
        p.paragraph_format.first_line_indent = Cm(0)
        p.paragraph_format.space_after = Pt(2)
        rk = p.add_run(k)
        format_run(rk, 13, bold=True)
        rv = p.add_run(v)
        format_run(rv, 13, bold=False)

    p_app = doc.add_paragraph()
    p_app.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    p_app.paragraph_format.first_line_indent = Cm(0)
    p_app.paragraph_format.space_before = Pt(8)
    p_app.paragraph_format.space_after = Pt(8)
    r_app = p_app.add_run(
        "ЗАТВЕРДЖУЮ:\nЗавідувач кафедри КІТ\n___________ Аліна САВЧЕНКО\n«___» _____________ 2026 р."
    )
    format_run(r_app, 13, bold=False)

    p_z = doc.add_paragraph()
    p_z.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_z.paragraph_format.first_line_indent = Cm(0)
    p_z.paragraph_format.space_before = Pt(6)
    p_z.paragraph_format.space_after = Pt(6)
    rz = p_z.add_run("ЗАВДАННЯ\nз курсової роботи здобувача\nКиян Діани, група M-F3-26-1-ВД")
    format_run(rz, 14, bold=True)

    task_points = [
        (
            "1. Тема роботи: ",
            "«Конвеєр обробки та аналізу великих даних пунктуальності авіарейсів і чинників затримок» (Варіант № 3).",
        ),
        ("2. Термін здачі здобувачем завершеної роботи: ", "згідно з календарним графіком навчального процесу 2026 р."),
        (
            "3. Вихідні дані до роботи: ",
            "відкритий набір даних пунктуальності комерційних авіарейсів US DOT Bureau of Transportation Statistics (BTS On-Time Performance, обсяг 200 700 записів, 31 атрибут), довідники світових авіакомпаній (24 перевізники) та глобальних вузлових аеропортів (47 хабів світу з геокоординатами WGS-84), жива потокова телеметрія повітряних суден OpenSky Network REST API; вимоги до побудови Lambda + Medallion конвеєра у середовищі Docker Compose.",
        ),
        (
            "4. Перелік питань, які повинні бути розроблені в роботі: ",
            "проаналізувати предметну область пунктуальності авіарейсів та чинників затримок за стандартами ICAO/FAA/BTS; обґрунтувати гібридну архітектуру Lambda та багатошарове сховище Medallion; розробити технічне завдання на програмно-аналітичний комплекс «ПАКАПА»; спроєктувати модель даних і багатофакторне ядро контролю якості (Data Quality Engine) з карантинним шаром; реалізувати пакетний ETL-конвеєр у PySpark, регресійне моделювання затримки прибуття у Spark MLlib, потоковий конвеєр реального часу OpenSky Network -> Apache Kafka -> PostgreSQL та багатосторінковий BI-дашборд; виконати комплексне модульне й інтеграційне тестування.",
        ),
        (
            "5. Перелік графічних матеріалів: ",
            "архітектурна схема компонентів і потоків даних комплексу; інформаційна ER-модель сховища даних; схема послідовності етапів ETL-конвеєра Medallion; UML-діаграма класів; UML-діаграми станів та послідовності взаємодії; графіки аналізу пунктуальності OTP-15, факторів затримок, важливості ознак Spark MLlib та BI-дашборду.",
        ),
    ]
    for num_title, body_txt in task_points:
        add_body_p(doc, body_txt, bold_prefix=num_title, indent_cm=0.0)

    add_body_p(doc, "", bold_prefix="6. Календарний план-графік виконання курсової роботи:", indent_cm=0.0)
    plan_headers = ["№ з/п", "Зміст етапу завдання (у наказовому способі)", "Термін виконання", "Відмітка про виконання"]
    plan_rows = [
        ["1", "Проаналізувати предметну область пунктуальності авіаперевезень, джерела даних US DOT BTS і OpenSky Network та літературні джерела", "1–2 тиждень", "Виконано"],
        ["2", "Розробити та обґрунтувати технічне завдання, паспорт набору даних і вимоги до комплексу «ПАКАПА»", "3–4 тиждень", "Виконано"],
        ["3", "Спроєктувати архітектуру Lambda + Medallion, схему аналітичної СУБД PostgreSQL, UML-діаграми та алгоритми Data Quality", "5–6 тиждень", "Виконано"],
        ["4", "Реалізувати пакетний конвеєр PySpark (Bronze, Silver, Gold, Quarantine) та регресійні моделі у Spark MLlib", "7–9 тиждень", "Виконано"],
        ["5", "Реалізувати потоковий конвеєр OpenSky API -> Apache Kafka та багатосторінковий BI-дашборд (Streamlit + Plotly)", "10–11 тиждень", "Виконано"],
        ["6", "Провести модульне та інтеграційне тестування (pytest), контрольний запуск і оформити пояснювальну записку", "12–13 тиждень", "Виконано"],
        ["7", "Захистити курсову роботу перед комісією кафедри", "14 тиждень", "Допущено"],
    ]
    add_styled_table(doc, "Б.1", "Календарний план-графік виконання курсової роботи", plan_headers, plan_rows, [1.5, 10.5, 2.8, 2.7], font_size=11)

    p_sign2 = doc.add_paragraph()
    p_sign2.alignment = WD_ALIGN_PARAGRAPH.LEFT
    p_sign2.paragraph_format.first_line_indent = Cm(0)
    p_sign2.paragraph_format.line_spacing_rule = WD_LINE_SPACING.ONE_POINT_FIVE
    r_sign2 = p_sign2.add_run(
        "7. Дата видачі завдання: «02» вересня 2026 р.\n"
        "Керівник курсової роботи: __________________ Костянтин ПРОКОПЕНКО\n"
        "Завдання прийняла до виконання: __________________ Діана КИЯН"
    )
    format_run(r_sign2, 13, bold=False)

    # -------------------------------------------------------------------------
    # 3. РЕФЕРАТ (за зразком Додатка В методички, стор. 26-27)
    # -------------------------------------------------------------------------
    add_chapter_heading(doc, "РЕФЕРАТ", new_page=True)
    add_body_p(
        doc,
        "Пояснювальна записка до курсової роботи «Конвеєр обробки та аналізу великих даних пунктуальності авіарейсів і чинників затримок»: 40 сторінок основного тексту, 16 рисунків, 11 таблиць, 20 використаних джерел, 3 додатки.",
    )
    add_body_p(
        doc,
        "розробка, контейнеризація та експериментальне дослідження відтворюваного програмно-аналітичного комплексу «ПАКАПА» для отримання, багатошарового зберігання, контролю якості, розподіленої пакетної та потокової обробки, машинного навчання й інтерактивної BI-візуалізації великих даних пунктуальності комерційних авіарейсів і чинників їх затримок.",
        bold_prefix="Мета курсової роботи – ",
    )
    add_body_p(doc, "Для досягнення поставленої мети курсової роботи визначені наступні завдання:")
    ref_tasks = [
        "проаналізувати специфіку авіаційної телеметрії та статистичної звітності пунктуальності польотів за методологією ICAO, FAA та US DOT BTS;",
        "розробити технічне завдання на створення програмно-аналітичного комплексу «ПАКАПА» відповідно до стандартів технічної документації;",
        "спроєктувати гібридну архітектуру платформи великих даних на базі поєднання парадигм Lambda (Batch + Speed Layer) та Medallion Lakehouse (Bronze, Silver, Gold, Quarantine);",
        "реалізувати у середовищі Apache Spark (PySpark) автоматизоване ядро контролю якості даних (Data Quality Engine) для виявлення та ізоляції 6 класів аномалій і порушень цілісності;",
        "побудувати 6 спеціалізованих аналітичних вітрин шару Gold у форматах Apache Parquet (Snappy) та реляційної СУБД PostgreSQL;",
        "реалізувати та дослідити регресійні моделі машинного навчання у бібліотеці Spark MLlib (Linear Regression ElasticNet та Random Forest Regressor) для прогнозування тривалості затримки прибуття рейсу (ARR_DELAY) та кількісного оцінювання важливості чинників затримок (Feature Importance);",
        "розробити потоковий конвеєр реального часу з інтеграцією відкритого REST API OpenSky Network та брокера повідомлень Apache Kafka (KRaft) із маршрутизацією пошкоджених пакетів у чергу Dead Letter Queue (DLQ);",
        "створити шестисторінковий інтерактивний BI-дашборд засобами Streamlit і Plotly та верифікувати відтворюваність комплексу пакетом автотестів pytest і розгортанням у Docker Compose.",
    ]
    for rt in ref_tasks:
        add_bullet_p(doc, rt)

    add_body_p(
        doc,
        "процеси генерації, накопичення, розподіленої пакетно-потокової обробки та інтелектуального аналізу великих масивів операційних і метеорологічних даних цивільної авіації, що відображають регулярність виконання польотів та каскадне поширення затримок у транспортній мережі аеропортів.",
        bold_prefix="Об’єкт дослідження – ",
    )
    add_body_p(
        doc,
        "методи, моделі та програмно-інфраструктурні засоби побудови відтворюваного конвеєра великих даних (Apache Spark, Apache Kafka, Parquet, PostgreSQL, Spark MLlib, BI) для оцінювання показників пунктуальності авіарейсів, факторної декомпозиції причин затримок та регресійного прогнозування відхилень від розкладу.",
        bold_prefix="Предмет дослідження – ",
    )
    add_body_p(
        doc,
        "методи розподілених обчислень та реляційної алгебри (Spark SQL, Broadcast Join, віконні функції), методи інженерії якості даних (правилова валідація, дедуплікація, карантинна ізоляція), методи статистичного та кореляційного аналізу, методи машинного навчання (регуляризована лінійна регресія ElasticNet, ансамбль регресійних дерев Random Forest), методи потокової обробки подій (Publish/Subscribe у Apache Kafka) та контейнеризації мікросервісів (Docker Compose).",
        bold_prefix="Методи дослідження – ",
    )
    add_body_p(
        doc,
        "ВЕЛИКІ ДАНІ, КОНВЕЄР ДАНИХ, APACHE SPARK, PYSPARK, APACHE KAFKA, АРХІТЕКТУРА LAMBDA, АРХІТЕКТУРА MEDALLION, ПУНКТУАЛЬНІСТЬ АВІАРЕЙСІВ, OTP-15, ЧИННИКИ ЗАТРИМОК, SPARK MLLIB, РЕГРЕСІЙНЕ ПРОГНОЗУВАННЯ, ЯКІСТЬ ДАНИХ, КАРАНТИН ДАНИХ, OPENSKY NETWORK, POSTGRESQL, BI-ДАШБОРД.",
        bold_prefix="КЛЮЧОВІ СЛОВА: ",
    )

    # -------------------------------------------------------------------------
    # 4. ЗМІСТ (оформлений згідно з вимогами ст. 8-9 методички)
    # -------------------------------------------------------------------------
    add_chapter_heading(doc, "ЗМІСТ", new_page=True)
    toc_entries = [
        ("ВСТУП", "5", 0),
        ("1 АНАЛІЗ ПРЕДМЕТНОЇ ОБЛАСТІ, ДЖЕРЕЛ ДАНИХ ТА ПОСТАНОВКА ЗАДАЧІ", "7", 0),
        ("1.1 Характеристика проблеми пунктуальності авіаперевезень і класифікація чинників затримок", "7", 1),
        ("1.2 Аналіз джерел даних US DOT BTS та потокової телеметрії OpenSky Network", "9", 1),
        ("1.3 Порівняльний аналіз архітектур великих даних та обґрунтування технологічного стеку", "11", 1),
        ("1.4 Інформаційна модель предметної області та формальна постановка задачі", "13", 1),
        ("2 РОЗРОБКА І ОБҐРУНТУВАННЯ ТЕХНІЧНОГО ЗАВДАННЯ", "15", 0),
        ("2.1 Основа для розробки", "15", 1),
        ("2.2 Цілі розробки", "15", 1),
        ("2.3 Вимоги до програми", "16", 1),
        ("2.4 Вимоги до надійності", "18", 1),
        ("2.5 Умови експлуатації та вимоги до складу і параметрів технічних засобів", "19", 1),
        ("2.6 Стадії і етапи розробки", "20", 1),
        ("2.7 Порядок контролю і прийому проєкту", "20", 1),
        ("3 ПРОЄКТУВАННЯ АРХІТЕКТУРИ ТА АЛГОРИТМІЧНОГО ЗАБЕЗПЕЧЕННЯ КОНВЕЄРА ДАНИХ", "22", 0),
        ("3.1 Архітектурна схема компонентів програмно-аналітичного комплексу та потоків даних", "22", 1),
        ("3.2 Модель багатошарового зберігання даних (Bronze, Silver, Gold, Quarantine) та схема СУБД", "23", 1),
        ("3.3 Алгоритмічне та математичне забезпечення контролю якості даних, розрахунку KPI та регресійного моделювання Spark MLlib", "25", 1),
        ("3.4 Об’єктно-орієнтоване та структурне проєктування системи (UML-діаграми класів, станів та взаємодії)", "27", 1),
        ("4 ПРАКТИЧНА РЕАЛІЗАЦІЯ, ТЕСТУВАННЯ ТА АНАЛІЗ РЕЗУЛЬТАТІВ", "29", 0),
        ("4.1 Програмна реалізація пакетного ETL-конвеєра PySpark, модуля Spark MLlib та потокового шару Apache Kafka", "29", 1),
        ("4.2 Контейнеризація середовища у Docker Compose, інструкція з розгортання та керування комплексом", "30", 1),
        ("4.3 Методика та результати автоматизованого тестування, аудиту якості даних і швидкодії конвеєра", "31", 1),
        ("4.4 Аналіз отриманих аналітичних результатів та інтерактивного BI-дашборду пунктуальності авіарейсів", "33", 1),
        ("ВИСНОВКИ", "41", 0),
        ("СПИСОК ВИКОРИСТАНИХ ДЖЕРЕЛ", "43", 0),
        ("ДОДАТОК А Паспорт набору даних та специфікація таблиць аналітичного сховища", "45", 0),
        ("ДОДАТОК Б Лістинги ключових програмних модулів конвеєра «ПАКАПА»", "47", 0),
        ("ДОДАТОК В Конфігурація середовища Docker Compose та результати автотестів", "54", 0),
    ]
    for title_txt, page_num, level in toc_entries:
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.LEFT
        pf = p.paragraph_format
        pf.first_line_indent = Cm(0.8 if level == 1 else 0.0)
        pf.line_spacing_rule = WD_LINE_SPACING.ONE_POINT_FIVE
        pf.space_before = Pt(0)
        pf.space_after = Pt(1)
        is_top = level == 0
        r_t = p.add_run(f"{title_txt} ")
        format_run(r_t, 13.5 if is_top else 13, bold=is_top)
        dots_count = max(4, 72 - len(title_txt) - (4 if level == 1 else 0))
        r_d = p.add_run("." * dots_count + f" {page_num}")
        format_run(r_d, 13, bold=is_top)
