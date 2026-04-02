from __future__ import annotations

import io
from datetime import datetime
from typing import Any

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel


router = APIRouter(prefix="/export", tags=["Export"])


class ExportPdfRequest(BaseModel):
    dataset_name: str | None = None
    query: str | None = None
    results: Any | None = None
    timestamp: str | None = None


@router.post("/pdf", summary="Export results as PDF")
def export_pdf(payload: ExportPdfRequest) -> StreamingResponse:
    """Generate a simple PDF report.

    Uses fpdf2 if available.
    """

    try:
        from fpdf import FPDF  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Missing dependency: fpdf2. Install it in backend/requirements.txt to enable PDF export."
        ) from exc

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, "AI Data Analyst Report", ln=True)

    pdf.set_font("Helvetica", size=11)
    dataset_line = f"Dataset: {payload.dataset_name or '—'}"
    pdf.multi_cell(0, 7, dataset_line)

    query_line = f"Query: {payload.query or '—'}"
    pdf.multi_cell(0, 7, query_line)

    ts = payload.timestamp or datetime.utcnow().isoformat()
    pdf.multi_cell(0, 7, f"Generated: {ts}")

    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, "Results", ln=True)

    pdf.set_font("Courier", size=9)
    result_text = ""
    try:
        import json

        result_text = json.dumps(payload.results, indent=2, default=str)
    except Exception:
        result_text = str(payload.results)

    pdf.multi_cell(0, 5, result_text)

    out = io.BytesIO(pdf.output(dest="S").encode("latin-1"))
    out.seek(0)

    return StreamingResponse(
        out,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=report.pdf"},
    )
