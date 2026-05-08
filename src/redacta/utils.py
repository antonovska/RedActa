from __future__ import annotations

import re
from datetime import date
from pathlib import Path

from .base_agent import compact


_MONTHS = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}


def build_source_document_label(amendment_doc: Path) -> str:
    label = amendment_doc.stem
    while label.startswith("изм_") or re.match(r"^изм\d*_", label):
        label = re.sub(r"^изм\d*_", "", label)
    label = re.sub(r"(?<=\d)_(?=\d)", ".", label)
    return label


def to_genitive(label: str) -> str:
    if label.startswith("Приказ "):
        return "Приказа " + label[len("Приказ "):]
    if label.startswith("Распоряжение "):
        return "Распоряжения " + label[len("Распоряжение "):]
    if label.startswith("Указание "):
        return "Указания " + label[len("Указание "):]
    if label.startswith("Решение "):
        return "Решения " + label[len("Решение "):]
    if label.startswith("Указ "):
        return "Указа " + label[len("Указ "):]
    return label


def to_instrumental(label: str) -> str:
    if label.startswith("Приказ "):
        return "Приказа " + label[len("Приказ "):]
    if label.startswith("Распоряжение "):
        return "Распоряжения " + label[len("Распоряжение "):]
    if label.startswith("Указ "):
        return "Указа " + label[len("Указ "):]
    if label.startswith("Указание "):
        return "Указания " + label[len("Указание "):]    
    if label.startswith("Решение "):
        return "Решения " + label[len("Решение "):]    
    return label


def format_revision_reference(label: str) -> str:
    value = compact(to_genitive(label))
    if not value:
        return ""
    return f"(в ред. {value})"


def extract_document_number(label: str) -> str:
    match = re.search(r"\bN\s*([0-9A-Za-zА-Яа-я./-]+)", label)
    return compact(match.group(1)) if match else ""


def extract_document_date(label: str) -> date | None:
    numeric = re.search(r"от\s+(\d{1,2})[._](\d{1,2})[._](\d{4})", label, flags=re.IGNORECASE)
    if numeric:
        day, month, year = map(int, numeric.groups())
        return date(year, month, day)
    textual = re.search(
        r"от\s+(\d{1,2})\s+([А-Яа-яёЁ]+)\s+(\d{4})",
        label,
        flags=re.IGNORECASE,
    )
    if textual:
        day = int(textual.group(1))
        month = _MONTHS.get(textual.group(2).lower())
        year = int(textual.group(3))
        if month:
            return date(year, month, day)
    return None


def sort_key_for_label(label: str) -> tuple:
    doc_date = extract_document_date(label)
    return (
        doc_date or date.max,
        label,
    )


def looks_short_list_item(text: str) -> bool:
    return len(text.split()) <= 8 and len(text) <= 120


def normalize_member_entry_from_inclusion(text: str) -> str:
    value = compact(text)
    if " - " not in value:
        return value
    name_part, role_part = value.split(" - ", 1)
    name_tokens = name_part.split()
    if len(name_tokens) >= 3:
        fixed_name = []
        for token in name_tokens[:3]:
            if token.endswith("ича") or token.endswith("ьича"):
                fixed_name.append(token[:-1])
            elif token.endswith("ла") or token.endswith("ра") or token.endswith("ва"):
                fixed_name.append(token[:-1])
            else:
                fixed_name.append(token)
        name_part = " ".join(fixed_name)
    replacements = {
        "заместителя начальника": "заместитель начальника",
        "начальника": "начальник",
        "консультанта": "консультант",
        "ведущего консультанта": "ведущий консультант",
    }
    role_value = role_part
    for old, new in replacements.items():
        if role_value.startswith(old):
            role_value = new + role_value[len(old):]
            break
    return f"{name_part} - {role_value}"


def surname_stem(value: str) -> str:
    token = compact(value).split()[0].lower() if compact(value) else ""
    if token.endswith(("а", "я")):
        return token[:-1]
    return token
