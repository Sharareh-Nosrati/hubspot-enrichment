from __future__ import annotations

import io
import os
import re
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
from dotenv import load_dotenv

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

import resolve_restaurants as rr

load_dotenv()


# =========================================================
# ENV CONFIG
# =========================================================
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "")
GOOGLE_OUTPUT_SHEET_NAME = os.getenv("GOOGLE_OUTPUT_SHEET_NAME", f"{GOOGLE_SHEET_NAME}_output" if GOOGLE_SHEET_NAME else "output")

HUBSPOT_TOKEN = os.getenv("HUBSPOT_TOKEN", "")
HUBSPOT_BATCH_LIMIT = int(os.getenv("HUBSPOT_BATCH_LIMIT", "50"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.0"))

# Optional Render/runtime paths
WORK_DIR = os.getenv("WORK_DIR", ".")
PDF_DIR = os.getenv("PDF_DIR", "generated_pdfs")
LOCAL_OUTPUT_XLSX = os.getenv("LOCAL_OUTPUT_XLSX", "output_with_pdf.xlsx")

# Optional HubSpot defaults
DEFAULT_HUBSPOT_OBJECT_TYPE = os.getenv("DEFAULT_HUBSPOT_OBJECT_TYPE", "contacts")
HUBSPOT_FILE_FOLDER_PATH = os.getenv("HUBSPOT_FILE_FOLDER_PATH", "/online-presence-reports")
HUBSPOT_FILE_ACCESS = os.getenv("HUBSPOT_FILE_ACCESS", "PRIVATE")

# Optional row filter
ONLY_PROCESS_ROWS_WITHOUT_STATUS = os.getenv("ONLY_PROCESS_ROWS_WITHOUT_STATUS", "false").lower() == "true"

# Google scopes
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


# =========================================================
# GENERIC HELPERS
# =========================================================
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_utc_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def yn(value: bool) -> str:
    return "Yes" if bool(value) else "No"


def pct_from_score(value: Optional[float]) -> str:
    try:
        return f"{round(float(value) * 100, 1)}%"
    except Exception:
        return "-"


def value_or_dash(value: Any) -> str:
    v = safe_str(value)
    return v if v else "-"


def safe_filename(value: str, max_len: int = 90) -> str:
    value = safe_str(value)
    value = re.sub(r"[^\w\s\-]", "", value, flags=re.UNICODE)
    value = re.sub(r"\s+", "_", value).strip("_")
    if not value:
        value = "lead"
    return value[:max_len]


def ensure_dir(path: str) -> str:
    Path(path).mkdir(parents=True, exist_ok=True)
    return path


def unique_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items:
        if not item:
            continue
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


# =========================================================
# GOOGLE SHEETS
# =========================================================
def get_google_sheets_service():
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise ValueError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")
    if not GOOGLE_SHEET_ID:
        raise ValueError("Missing GOOGLE_SHEET_ID")

    try:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    except json.JSONDecodeError as e:
        raise ValueError(f"GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON: {e}") from e

    credentials = service_account.Credentials.from_service_account_info(
        info,
        scopes=GOOGLE_SCOPES,
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def ensure_sheet_exists(service, spreadsheet_id: str, sheet_name: str) -> None:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing_names = {
        s["properties"]["title"]
        for s in meta.get("sheets", [])
    }

    if sheet_name in existing_names:
        return

    body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": sheet_name
                    }
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body,
    ).execute()


def read_sheet_as_dataframe(service, spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=sheet_name,
    ).execute()

    values = result.get("values", [])
    if not values:
        return pd.DataFrame()

    headers = [safe_str(x).lower() for x in values[0]]
    rows = values[1:]

    normalized_rows: List[List[str]] = []
    for row in rows:
        row_extended = list(row) + [""] * (len(headers) - len(row))
        normalized_rows.append(row_extended[:len(headers)])

    df = pd.DataFrame(normalized_rows, columns=headers)
    return df


def write_dataframe_to_sheet(service, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame) -> None:
    ensure_sheet_exists(service, spreadsheet_id, sheet_name)

    values: List[List[Any]] = [list(df.columns)]
    for _, row in df.iterrows():
        values.append([row[col] for col in df.columns])

    clear_range = f"{sheet_name}!A:ZZ"
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=clear_range,
        body={},
    ).execute()

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


# =========================================================
# BUSINESS SUMMARY HELPERS
# =========================================================
def extra_directory_links(result: rr.ResolveResult) -> List[str]:
    all_links = rr.json_list(result.directory_links_json)
    known = {
        result.google_maps_url,
        result.justeat_url,
        result.deliveroo_url,
        result.thefork_url,
        result.tripadvisor_url,
        result.glovo_url,
        result.restaurantguru_url,
        result.opentable_url,
        result.quandoo_url,
    }
    return [x for x in all_links if x and x not in known]


def website_search_candidates_with_scores(
    result: rr.ResolveResult,
    max_items: int = 5,
) -> List[Dict[str, Any]]:
    urls = rr.json_list(result.official_website_candidates_json)
    urls = unique_keep_order(urls)[:max_items]

    scored: List[Dict[str, Any]] = []
    for url in urls:
        try:
            details = rr.website_validation_details(result.name, result.city, url)
            scored.append({
                "url": url,
                "score": float(details.get("score", 0.0)),
                "is_valid": bool(details.get("is_valid", False)),
                "reason": details.get("reason", ""),
            })
        except Exception as e:
            scored.append({
                "url": url,
                "score": 0.0,
                "is_valid": False,
                "reason": f"validation_error: {e}",
            })

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored


def calculate_total_score(result: rr.ResolveResult) -> int:
    score = 0.0

    if result.website:
        score += 24
    if result.instagram:
        score += 8
    if result.facebook:
        score += 8
    if result.tiktok:
        score += 8
    if result.google_maps_url:
        score += 12
    if result.threads:
        score += 4
    if result.x:
        score += 4
    if result.youtube:
        score += 4

    if result.menu_present:
        score += 6
    if result.booking_present:
        score += 5
    if result.delivery_present:
        score += 5
    if result.data_capture_present:
        score += 4
    if result.contact_present:
        score += 5

    if result.thefork_url:
        score += 4
    if result.tripadvisor_url:
        score += 4
    if result.opentable_url:
        score += 3
    if result.justeat_url:
        score += 2
    if result.deliveroo_url:
        score += 2
    if result.glovo_url:
        score += 2
    if result.restaurantguru_url:
        score += 1
    if result.quandoo_url:
        score += 1

    try:
        score += min(max(float(result.website_validation_score), 0.0), 1.0) * 6
    except Exception:
        pass

    return max(0, min(100, round(score)))


def generate_hubspot_note_summary(
    result: rr.ResolveResult,
    generated_at: str,
    total_score: int,
    confidence_total_percentage: float,
) -> str:
    search_candidates = website_search_candidates_with_scores(result, max_items=5)
    search_lines = []
    for item in search_candidates:
        search_lines.append(f"- Website: {item['url']}, {round(item['score'] * 100, 1)}%")

    extra_links = extra_directory_links(result)
    extra_links_text = "\n".join([f"- {x}" for x in extra_links[:10]]) if extra_links else "-"

    note = f"""
Online Presence Summary

Lead: {result.name}
Website: {result.website or "-"}
Generated: {generated_at}
Total score: {total_score}/100
Confidence total percentage: {round(confidence_total_percentage, 1)}%

Search links (when website is missing):
{chr(10).join(search_lines) if search_lines else "-"}

Website checks:
- Menu: {yn(result.menu_present)}
- Booking: {yn(result.booking_present)}
- Delivery: {yn(result.delivery_present)}
- Data capture: {yn(result.data_capture_present)}
- Contact info: {yn(result.contact_present)}
- Website creator: {result.website_creator or result.website_platform or "-"}

Presence links:
- Instagram: {result.instagram or "-"}, {pct_from_score(result.instagram_score)}
- Facebook: {result.facebook or "-"}, {pct_from_score(result.facebook_score)}
- TikTok: {result.tiktok or "-"}, {pct_from_score(result.tiktok_score)}
- Google Maps: {result.google_maps_url or "-"}, {pct_from_score(0.85 if result.google_maps_url else 0.0)}
- Threads: {result.threads or "-"}, {pct_from_score(result.threads_score)}
- X: {result.x or "-"}, {pct_from_score(result.x_score)}

Other general links:
- TheFork: {result.thefork_url or "-"}
- TripAdvisor: {result.tripadvisor_url or "-"}
- OpenTable: {result.opentable_url or "-"}
- Extra links:
{extra_links_text}
""".strip()

    return note


# =========================================================
# PDF GENERATION
# =========================================================
def build_pdf_styles():
    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name="DocTitle",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=19,
        leading=23,
        textColor=colors.HexColor("#17324D"),
        spaceAfter=8,
    ))
    styles.add(ParagraphStyle(
        name="Meta",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9.5,
        leading=12,
        textColor=colors.HexColor("#5A6470"),
        spaceAfter=2,
    ))
    styles.add(ParagraphStyle(
        name="SectionTitle",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=11.5,
        leading=14,
        textColor=colors.HexColor("#17324D"),
        spaceBefore=7,
        spaceAfter=5,
    ))
    styles.add(ParagraphStyle(
        name="Cell",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8.8,
        leading=11,
        textColor=colors.black,
        alignment=TA_LEFT,
    ))
    styles.add(ParagraphStyle(
        name="LinkCell",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8.5,
        leading=10.8,
        textColor=colors.HexColor("#0B57D0"),
        alignment=TA_LEFT,
    ))
    return styles


def make_table(rows: List[List[Any]], col_widths: List[float]) -> Table:
    tbl = Table(rows, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EAF1F8")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#17324D")),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("LEADING", (0, 0), (-1, -1), 11),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#CBD4E1")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FBFE")]),
    ]))
    return tbl


def create_lead_summary_pdf_bytes(
    result: rr.ResolveResult,
    generated_at: str,
    total_score: int,
    confidence_total_percentage: float,
) -> bytes:
    styles = build_pdf_styles()
    buffer = io.BytesIO()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=15 * mm,
        rightMargin=15 * mm,
        topMargin=15 * mm,
        bottomMargin=15 * mm,
        title=f"Online Presence Summary - {result.name}",
    )

    story = []

    story.append(Paragraph("Online Presence Summary", styles["DocTitle"]))
    story.append(Paragraph(f"<b>Lead:</b> {value_or_dash(result.name)}", styles["Meta"]))
    story.append(Paragraph(f"<b>Website:</b> {value_or_dash(result.website)}", styles["Meta"]))
    story.append(Paragraph(f"<b>Generated:</b> {generated_at}", styles["Meta"]))
    story.append(Spacer(1, 6))

    top_rows = [
        [Paragraph("Metric", styles["Cell"]), Paragraph("Value", styles["Cell"])],
        [Paragraph("Total score", styles["Cell"]), Paragraph(f"{total_score}/100", styles["Cell"])],
        [Paragraph("Confidence total percentage", styles["Cell"]), Paragraph(f"{round(confidence_total_percentage, 1)}%", styles["Cell"])],
    ]
    story.append(make_table(top_rows, [62 * mm, 108 * mm]))
    story.append(Spacer(1, 8))

    if not result.website:
        search_candidates = website_search_candidates_with_scores(result, max_items=5)
        story.append(Paragraph("Search links (when website is missing)", styles["SectionTitle"]))
        search_rows = [[Paragraph("Website", styles["Cell"]), Paragraph("Confidence", styles["Cell"])]]
        if search_candidates:
            for item in search_candidates:
                search_rows.append([
                    Paragraph(value_or_dash(item["url"]), styles["LinkCell"]),
                    Paragraph(f"{round(item['score'] * 100, 1)}%", styles["Cell"]),
                ])
        else:
            search_rows.append([Paragraph("-", styles["Cell"]), Paragraph("-", styles["Cell"])])
        story.append(make_table(search_rows, [138 * mm, 32 * mm]))
        story.append(Spacer(1, 8))

    story.append(Paragraph("Website checks", styles["SectionTitle"]))
    website_rows = [
        [Paragraph("Check", styles["Cell"]), Paragraph("Value", styles["Cell"])],
        [Paragraph("Menu", styles["Cell"]), Paragraph(yn(result.menu_present), styles["Cell"])],
        [Paragraph("Booking", styles["Cell"]), Paragraph(yn(result.booking_present), styles["Cell"])],
        [Paragraph("Delivery", styles["Cell"]), Paragraph(yn(result.delivery_present), styles["Cell"])],
        [Paragraph("Data capture", styles["Cell"]), Paragraph(yn(result.data_capture_present), styles["Cell"])],
        [Paragraph("Contact info", styles["Cell"]), Paragraph(yn(result.contact_present), styles["Cell"])],
        [Paragraph("Website creator", styles["Cell"]), Paragraph(value_or_dash(result.website_creator or result.website_platform), styles["Cell"])],
    ]
    story.append(make_table(website_rows, [60 * mm, 110 * mm]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Presence links", styles["SectionTitle"]))
    presence_rows = [
        [Paragraph("Channel", styles["Cell"]), Paragraph("Link", styles["Cell"]), Paragraph("Confidence", styles["Cell"])],
        [Paragraph("Instagram", styles["Cell"]), Paragraph(value_or_dash(result.instagram), styles["LinkCell"]), Paragraph(pct_from_score(result.instagram_score), styles["Cell"])],
        [Paragraph("Facebook", styles["Cell"]), Paragraph(value_or_dash(result.facebook), styles["LinkCell"]), Paragraph(pct_from_score(result.facebook_score), styles["Cell"])],
        [Paragraph("TikTok", styles["Cell"]), Paragraph(value_or_dash(result.tiktok), styles["LinkCell"]), Paragraph(pct_from_score(result.tiktok_score), styles["Cell"])],
        [Paragraph("Google Maps", styles["Cell"]), Paragraph(value_or_dash(result.google_maps_url), styles["LinkCell"]), Paragraph(pct_from_score(0.85 if result.google_maps_url else 0.0), styles["Cell"])],
        [Paragraph("Threads", styles["Cell"]), Paragraph(value_or_dash(result.threads), styles["LinkCell"]), Paragraph(pct_from_score(result.threads_score), styles["Cell"])],
        [Paragraph("X", styles["Cell"]), Paragraph(value_or_dash(result.x), styles["LinkCell"]), Paragraph(pct_from_score(result.x_score), styles["Cell"])],
    ]
    story.append(make_table(presence_rows, [28 * mm, 112 * mm, 30 * mm]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Other general links", styles["SectionTitle"]))
    extra_links = extra_directory_links(result)
    extra_links_text = "<br/>".join(extra_links[:12]) if extra_links else "-"

    other_rows = [
        [Paragraph("Item", styles["Cell"]), Paragraph("Value", styles["Cell"])],
        [Paragraph("TheFork", styles["Cell"]), Paragraph(value_or_dash(result.thefork_url), styles["LinkCell"])],
        [Paragraph("TripAdvisor", styles["Cell"]), Paragraph(value_or_dash(result.tripadvisor_url), styles["LinkCell"])],
        [Paragraph("OpenTable", styles["Cell"]), Paragraph(value_or_dash(result.opentable_url), styles["LinkCell"])],
        [Paragraph("Extra links", styles["Cell"]), Paragraph(extra_links_text, styles["LinkCell"])],
    ]
    story.append(make_table(other_rows, [42 * mm, 128 * mm]))

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes


# =========================================================
# HUBSPOT API
# =========================================================
def hubspot_headers() -> Dict[str, str]:
    if not HUBSPOT_TOKEN:
        raise ValueError("Missing HUBSPOT_TOKEN")
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    }


def get_note_association_type_id(to_object_type: str) -> int:
    """
    Try to fetch dynamically.
    Fall back to default contact association type 202 if object is contacts.
    """
    url = f"https://api.hubapi.com/crm/v4/associations/notes/{to_object_type}/labels"
    resp = requests.get(url, headers=hubspot_headers(), timeout=30)

    if resp.ok:
        data = resp.json()
        results = data.get("results", [])
        if results:
            for item in results:
                if item.get("category") == "HUBSPOT_DEFINED":
                    type_id = item.get("typeId")
                    if type_id:
                        return int(type_id)
            type_id = results[0].get("typeId")
            if type_id:
                return int(type_id)

    if to_object_type == "contacts":
        return 202

    raise RuntimeError(f"Could not determine note associationTypeId for object type '{to_object_type}'")


def upload_pdf_to_hubspot(pdf_bytes: bytes, filename: str) -> Dict[str, Any]:
    url = "https://api.hubapi.com/files/v3/files"

    files = {
        "file": (filename, pdf_bytes, "application/pdf"),
    }
    data = {
        "folderPath": HUBSPOT_FILE_FOLDER_PATH,
        "fileName": filename,
        "options": json.dumps({
            "access": HUBSPOT_FILE_ACCESS,
            "duplicateValidationStrategy": "RETURN_EXISTING",
            "duplicateValidationScope": "ENTIRE_PORTAL",
        }),
    }

    resp = requests.post(
        url,
        headers=hubspot_headers(),
        files=files,
        data=data,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def create_hubspot_note_with_attachment(
    hubspot_object_id: str,
    hubspot_object_type: str,
    note_body: str,
    file_id: str,
    timestamp_iso: str,
) -> Dict[str, Any]:
    association_type_id = get_note_association_type_id(hubspot_object_type)

    url = "https://api.hubapi.com/crm/v3/objects/notes"
    body = {
        "associations": [
            {
                "to": {"id": str(hubspot_object_id)},
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": association_type_id,
                    }
                ],
            }
        ],
        "properties": {
            "hs_note_body": note_body,
            "hs_timestamp": timestamp_iso,
            "hs_attachment_ids": str(file_id),
        },
    }

    resp = requests.post(
        url,
        headers={**hubspot_headers(), "Content-Type": "application/json"},
        json=body,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# =========================================================
# CACHE / RECORD REBUILD
# =========================================================
def resolve_result_from_record(name: str, city: str, country: str, record: Dict[str, Any]) -> rr.ResolveResult:
    result = rr.ResolveResult(name=name, city=city, country=country)
    for field_name in result.__dataclass_fields__.keys():
        if field_name in record:
            setattr(result, field_name, record[field_name])
    return result


# =========================================================
# PROCESS ONE ROW
# =========================================================
def process_row(row: Dict[str, Any], cache: Dict[str, Any], pdf_dir: str) -> Dict[str, Any]:
    name = safe_str(row.get("name", ""))
    city = safe_str(row.get("city", ""))
    country = safe_str(row.get("country", ""))

    if not name or not city or not country:
        empty = rr.build_empty_record(row)
        empty["pdf_summary_path"] = ""
        empty["hubspot_note_summary"] = ""
        empty["hubspot_file_id"] = ""
        empty["hubspot_note_id"] = ""
        empty["total_score"] = 0
        empty["confidence_total_percentage"] = 0.0
        empty["generated_at"] = now_utc_text()
        empty["hubspot_status"] = "skipped_missing_required_fields"
        return empty

    key = rr.cache_key(name, city, country)

    if key in cache:
        record = cache[key]
        result = resolve_result_from_record(name, city, country, record)
        status = record.get("status", "ok")
    else:
        result = rr.resolve_one(name, city, country)

        status = "ok"
        if result.needs_review:
            status = "needs_review"

        if (
            ("timeout" in result.evidence.lower()) or
            ("request error" in result.evidence.lower())
        ) and not (result.website or result.instagram or result.facebook or result.tiktok):
            status = "error"

        record = rr.result_to_record(result, status)
        cache[key] = record
        rr.save_cache(cache)

    generated_at = now_utc_text()
    generated_at_iso = now_utc_iso()

    total_score = calculate_total_score(result)
    confidence_total_percentage = round(float(result.confidence) * 100, 1)

    hubspot_note_summary = generate_hubspot_note_summary(
        result=result,
        generated_at=generated_at,
        total_score=total_score,
        confidence_total_percentage=confidence_total_percentage,
    )

    pdf_filename = f"{safe_filename(result.name)}_{safe_filename(result.city)}.pdf"
    pdf_path = str(Path(pdf_dir) / pdf_filename)
    pdf_bytes = create_lead_summary_pdf_bytes(
        result=result,
        generated_at=generated_at,
        total_score=total_score,
        confidence_total_percentage=confidence_total_percentage,
    )

    with open(pdf_path, "wb") as f:
        f.write(pdf_bytes)

    hubspot_object_id = safe_str(
        row.get("hubspot_object_id")
        or row.get("hs_object_id")
        or row.get("hubspot_id")
        or row.get("record_id")
    )
    hubspot_object_type = safe_str(
        row.get("hubspot_object_type")
        or row.get("object_type")
        or DEFAULT_HUBSPOT_OBJECT_TYPE
    ).lower()

    hubspot_file_id = ""
    hubspot_note_id = ""
    hubspot_status = "not_sent_to_hubspot"

    if HUBSPOT_TOKEN and hubspot_object_id:
        try:
            uploaded = upload_pdf_to_hubspot(pdf_bytes, pdf_filename)
            hubspot_file_id = str(uploaded.get("id", "") or uploaded.get("fileId", ""))

            note_resp = create_hubspot_note_with_attachment(
                hubspot_object_id=hubspot_object_id,
                hubspot_object_type=hubspot_object_type,
                note_body=hubspot_note_summary,
                file_id=hubspot_file_id,
                timestamp_iso=generated_at_iso,
            )
            hubspot_note_id = safe_str(note_resp.get("id"))
            hubspot_status = "note_created_with_attachment"
        except Exception as e:
            hubspot_status = f"hubspot_error: {e}"
    elif not HUBSPOT_TOKEN:
        hubspot_status = "missing_hubspot_token"
    else:
        hubspot_status = "missing_hubspot_object_id"

    final_record = dict(record)
    final_record["pdf_summary_path"] = pdf_path
    final_record["hubspot_note_summary"] = hubspot_note_summary
    final_record["hubspot_file_id"] = hubspot_file_id
    final_record["hubspot_note_id"] = hubspot_note_id
    final_record["total_score"] = total_score
    final_record["confidence_total_percentage"] = confidence_total_percentage
    final_record["generated_at"] = generated_at
    final_record["hubspot_status"] = hubspot_status
    final_record["hubspot_object_id"] = hubspot_object_id
    final_record["hubspot_object_type"] = hubspot_object_type

    return {**row, **final_record}


# =========================================================
# MAIN JOB
# =========================================================
def validate_env() -> None:
    required = {
        "GOOGLE_SERVICE_ACCOUNT_JSON": GOOGLE_SERVICE_ACCOUNT_JSON,
        "GOOGLE_SHEET_ID": GOOGLE_SHEET_ID,
        "GOOGLE_SHEET_NAME": GOOGLE_SHEET_NAME,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")


def main() -> None:
    validate_env()

    work_dir = Path(WORK_DIR)
    pdf_dir = ensure_dir(str(work_dir / PDF_DIR))
    local_output_xlsx = str(work_dir / LOCAL_OUTPUT_XLSX)

    print("Starting Render job...")
    print(f"Google sheet id: {GOOGLE_SHEET_ID}")
    print(f"Source sheet: {GOOGLE_SHEET_NAME}")
    print(f"Output sheet: {GOOGLE_OUTPUT_SHEET_NAME}")
    print(f"PDF dir: {pdf_dir}")

    service = get_google_sheets_service()
    df = read_sheet_as_dataframe(service, GOOGLE_SHEET_ID, GOOGLE_SHEET_NAME)

    if df.empty:
        print("Source Google Sheet is empty. Nothing to process.")
        return

    required_cols = {"name", "city", "country"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required sheet columns: {sorted(missing_cols)}")

    cache = rr.load_cache()
    out_rows: List[Dict[str, Any]] = []

    processed_count = 0
    for _, row in df.iterrows():
        row_dict = row.to_dict()

        if ONLY_PROCESS_ROWS_WITHOUT_STATUS and safe_str(row_dict.get("hubspot_status")):
            out_rows.append(row_dict)
            continue

        try:
            processed = process_row(row_dict, cache, pdf_dir)
        except Exception as e:
            processed = dict(row_dict)
            processed["hubspot_status"] = f"row_processing_error: {e}"
            processed["generated_at"] = now_utc_text()

        out_rows.append(processed)
        processed_count += 1

        if processed_count % max(1, HUBSPOT_BATCH_LIMIT) == 0:
            time.sleep(REQUEST_DELAY)

        time.sleep(REQUEST_DELAY)

    out_df = pd.DataFrame(out_rows)
    out_df.to_excel(local_output_xlsx, index=False)

    write_dataframe_to_sheet(
        service=service,
        spreadsheet_id=GOOGLE_SHEET_ID,
        sheet_name=GOOGLE_OUTPUT_SHEET_NAME,
        df=out_df,
    )

    print(f"Saved local Excel: {local_output_xlsx}")
    print(f"Updated Google output sheet: {GOOGLE_OUTPUT_SHEET_NAME}")
    print("Render job completed successfully.")


if __name__ == "__main__":
    main()