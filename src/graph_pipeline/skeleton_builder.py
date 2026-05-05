from __future__ import annotations

import shutil
from pathlib import Path

from docx import Document

from .schema import AmendmentAnalysis, BaseAnalysis, ServiceTableSpec
from .service_tables import build_service_table_specs, insert_service_tables


class SkeletonBuilder:
    def build(
        self,
        base_doc: Path,
        base_analysis: BaseAnalysis,
        amendment_analyses: list[AmendmentAnalysis],
        output_doc: Path,
    ) -> list[ServiceTableSpec]:
        output_doc.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(base_doc, output_doc)
        document = Document(output_doc)
        specs = build_service_table_specs(base_analysis, amendment_analyses)
        insert_service_tables(document, specs)
        document.save(output_doc)
        return specs

