from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Iterable
from zipfile import ZipFile
import xml.etree.ElementTree as ET


@dataclass(frozen=True)
class CorpCodeRecord:
    corp_code: str
    corp_name: str
    corp_eng_name: str | None
    stock_code: str | None
    modify_date: str | None


def extract_corp_code_xml(data: bytes) -> bytes:
    """corpCode.zip(ZIP binary)에서 내부 XML 원문 bytes를 추출한다.

    상장사 명단 수집기는 '압축해제 후 남은 파일' 자체를 Bronze에 보존해야 하므로,
    파싱과 별개로 추출된 XML bytes를 그대로 다룰 수 있게 분리한다.
    """
    with ZipFile(BytesIO(data)) as zip_file:
        xml_names = [name for name in zip_file.namelist() if name.lower().endswith(".xml")]
        if not xml_names:
            raise ValueError("corpCode zip does not contain an XML file")
        return zip_file.read(xml_names[0])


def parse_corp_code_xml(xml_bytes: bytes) -> list[CorpCodeRecord]:
    root = ET.fromstring(xml_bytes)
    records: list[CorpCodeRecord] = []
    for node in root.findall(".//list"):
        stock_code = _text(node, "stock_code")
        records.append(
            CorpCodeRecord(
                corp_code=_required_text(node, "corp_code"),
                corp_name=_required_text(node, "corp_name"),
                corp_eng_name=_text(node, "corp_eng_name"),
                stock_code=stock_code if stock_code and stock_code.strip() else None,
                modify_date=_text(node, "modify_date"),
            )
        )
    return records


def parse_corp_code_zip(data: bytes) -> list[CorpCodeRecord]:
    return parse_corp_code_xml(extract_corp_code_xml(data))


def listed_company_records(records: Iterable[CorpCodeRecord]) -> list[CorpCodeRecord]:
    """corpCode.xml 레코드 중 종목코드가 있는, 즉 현재 상장된 회사만 남긴다.

    corpCode.xml은 비상장 법인까지 포함하므로 stock_code 유무가 상장 여부의 1차 기준이다.
    """
    return [record for record in records if record.stock_code]


def _text(node: ET.Element, tag: str) -> str | None:
    child = node.find(tag)
    if child is None or child.text is None:
        return None
    return child.text.strip()


def _required_text(node: ET.Element, tag: str) -> str:
    value = _text(node, tag)
    if not value:
        raise ValueError(f"missing corpCode field: {tag}")
    return value

