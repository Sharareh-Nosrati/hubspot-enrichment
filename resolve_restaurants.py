import os
import time
import json
import tempfile
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import requests
import gspread
from fpdf import FPDF
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from resolve_restaurants import resolve_one


load_dotenv()

HUBSPOT_TOKEN = os.getenv("HUBSPOT_TOKEN", "")
BASE_URL = "https://api.hubapi.com"

OBJECT_TYPE = "contacts"

PROP_CITY = "city"
PROP_COUNTRY = "country"

POLL_LIMIT = int(os.getenv("HUBSPOT_BATCH_LIMIT", "2"))
SLEEP_BETWEEN_RECORDS = float(os.getenv("REQUEST_DELAY", "1"))

GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")

_gspread_client = None
_worksheet = None

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

HEADERS = [
    "hubspot_company_id",
    "name",
    "city",
    "country",
    "website",
    "instagram",
    "facebook",
    "tiktok",
    "tiktok_present",
    "menu_present",
    "booking_present",
    "delivery_present",
    "data_capture_present",
    "contact_present",
    "confidence",
    "source",
    "status",
    "needs_review",
    "is_restaurant_match",
    "non_restaurant_reason",
    "evidence",
    "last_checked",
    "hubspot_file_id",
    "pdf_url"
]





def hs_headers() -> Dict[str, str]:
    if not HUBSPOT_TOKEN:
        raise ValueError("HUBSPOT_TOKEN is missing.")
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json"
    }


def get_gspread_client():
    global _gspread_client

    if _gspread_client is not None:
        return _gspread_client

    google_service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

    print("DEBUG GOOGLE_SERVICE_ACCOUNT_JSON exists:", bool(google_service_account_json))
    print("DEBUG GOOGLE_SHEET_NAME:", repr(GOOGLE_SHEET_NAME))
    print("DEBUG GOOGLE_SHEET_ID:", repr(GOOGLE_SHEET_ID))

    if not google_service_account_json:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON is missing.")

    try:
        service_account_info = json.loads(google_service_account_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON: {e}")

    creds = Credentials.from_service_account_info(
        service_account_info,
        scopes=SCOPES
    )

    _gspread_client = gspread.authorize(creds)
    return _gspread_client


def get_worksheet():
    global _worksheet

    if _worksheet is not None:
        return _worksheet

    client = get_gspread_client()

    if GOOGLE_SHEET_ID:
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
    elif GOOGLE_SHEET_NAME:
        sheet = client.open(GOOGLE_SHEET_NAME)
    else:
        raise ValueError("Either GOOGLE_SHEET_ID or GOOGLE_SHEET_NAME must be set.")

    ws = sheet.sheet1

    existing_headers = ws.row_values(1)

    if not existing_headers:
        ws.append_row(HEADERS)
    elif existing_headers != HEADERS:
        ws.update(values=[HEADERS], range_name="A1:X1")

    _worksheet = ws
    return _worksheet


def get_existing_company_ids(ws) -> set[str]:
    values = ws.col_values(1)
    return {v for v in values[1:] if v}


def find_row_by_company_id(ws, company_id: str):
    if ws is None:
        raise ValueError("Worksheet is None in find_row_by_company_id().")

    values = ws.get_all_values()
    for idx, row in enumerate(values[1:], start=2):
        if row and row[0] == str(company_id):
            return idx
    return None


def already_processed(existing_ids: set[str], company_id: str) -> bool:
    return str(company_id) in existing_ids


def hubspot_get_signed_file_url(file_id: str) -> Optional[str]:
    url = f"{BASE_URL}/files/v3/files/{file_id}/signed-url"

    r = requests.get(url, headers=hs_headers(), timeout=30)

    if not r.ok:
        print("HubSpot signed URL error:")
        print(r.status_code)
        print(r.text)
        return None

    data = r.json()
    return data.get("url")


def upsert_company_result(
    ws,
    company_id: str,
    name: str,
    city: str,
    country: str,
    result=None,
    hubspot_file_id: str = "",
    pdf_url: str = "",
    status_override: str = "",
    needs_review_override: str = "",
    evidence_override: str = ""
) -> None:
    pdf_cell_value = pdf_url or ""

    if result is not None:
        status = "ok"
        evidence_text = result.evidence or ""

        if result.needs_review:
            status = "needs_review"

        if (
            ("timeout" in evidence_text.lower()) or
            ("request error" in evidence_text.lower())
        ) and not (result.website or result.instagram or result.facebook):
            status = "error"

        if status_override:
            status = status_override

        needs_review_value = needs_review_override or str(result.needs_review).lower()
        evidence_value = evidence_override or evidence_text

        row = [
            str(company_id),
            name,
            city,
            country,
            result.website or "",
            result.instagram or "",
            result.facebook or "",
            result.tiktok or "",
            str(result.tiktok_present).lower(),
            str(result.menu_present).lower(),
            str(result.booking_present).lower(),
            str(result.delivery_present).lower(),
            str(result.data_capture_present).lower(),
            str(result.contact_present).lower(),
            str(round(result.confidence, 3)),
            result.source or "",
            status,
            needs_review_value,
            str(result.is_restaurant_match).lower(),
            result.non_restaurant_reason or "",
            evidence_value,
            datetime.now(timezone.utc).isoformat(),
            hubspot_file_id or "",
            pdf_cell_value
        ]
    else:
        row = [
            str(company_id),
            name,
            city,
            country,
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            status_override or "no_requirements",
            needs_review_override or "true",
            "",
            "",
            evidence_override or "Missing required fields: name and/or city",
            datetime.now(timezone.utc).isoformat(),
            hubspot_file_id or "",
            pdf_cell_value
        ]

    existing_row = find_row_by_company_id(ws, str(company_id))
    if existing_row:
        ws.update(
            values=[row],
            range_name=f"A{existing_row}:X{existing_row}",
            value_input_option="USER_ENTERED"
        )
    else:
        ws.append_row(row, value_input_option="USER_ENTERED")


def hubspot_list_contacts(limit: int = 2) -> List[Dict[str, Any]]:
    properties = ["company", "firstname", "lastname", "city", "country", "createdate"]

    url = f"{BASE_URL}/crm/v3/objects/{OBJECT_TYPE}/search"

    payload = {
        "limit": limit,
        "properties": properties,
        "sorts": [
            {
                "propertyName": "createdate",
                "direction": "DESCENDING"
            }
        ]
    }

    r = requests.post(url, headers=hs_headers(), json=payload, timeout=30)

    if not r.ok:
        print("HubSpot contact search error:")
        print(r.status_code)
        print(r.text)

    r.raise_for_status()

    data = r.json()
    return data.get("results", [])


def hubspot_create_note_for_contact(
    record_id: str,
    note_body: str,
    attachment_ids: Optional[List[str]] = None
) -> Optional[str]:
    create_url = f"{BASE_URL}/crm/v3/objects/notes"

    properties = {
        "hs_note_body": note_body,
        "hs_timestamp": datetime.now(timezone.utc).isoformat()
    }

    if attachment_ids:
        properties["hs_attachment_ids"] = ";".join(str(x) for x in attachment_ids if x)

    note_payload = {
        "properties": properties,
        "associations": [
            {
                "to": {"id": str(record_id)},
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": 202
                    }
                ]
            }
        ]
    }

    r = requests.post(create_url, headers=hs_headers(), json=note_payload, timeout=30)

    if not r.ok:
        print("HubSpot note create error:")
        print(r.status_code)
        print(r.text)

    r.raise_for_status()
    note = r.json()
    return note.get("id")


def bool_to_yes_no(value: bool) -> str:
    return "Yes" if value else "No"


def html_link(url: str, label: str) -> str:
    if not url:
        return "Not found"
    return f'<a href="{url}" target="_blank" rel="noopener noreferrer">{label}</a>'


def build_note_body(result, name: str, city: str, country: str) -> str:
    status = "ok"
    evidence_text = result.evidence or ""

    if result.needs_review:
        status = "needs_review"

    if (
        ("timeout" in evidence_text.lower()) or
        ("request error" in evidence_text.lower())
    ) and not (result.website or result.instagram or result.facebook):
        status = "error"

    no_profile_message = ""
    if not (result.website or result.instagram or result.facebook or result.tiktok):
        no_profile_message = (
            "<b>Search Result:</b> No official website or social profiles were found "
            "after trying multiple discovery methods (OSM, guessed domain, and search providers).<br><br>"
        )

    return (
        f"<b>Online Presence Analysis</b><br><br>"
        f"{no_profile_message}"
        f"<b>Confidence:</b> {round(result.confidence, 3)}<br>"
        f"<b>Needs review:</b> {bool_to_yes_no(result.needs_review)}<br>"
        f"<b>Status:</b> {status}<br>"
        f"<b>Restaurant match:</b> {bool_to_yes_no(result.is_restaurant_match)}<br>"
        f"<b>Non-restaurant reason:</b> {result.non_restaurant_reason or 'N/A'}<br><br>"
        f"<b>Website:</b> {html_link(result.website, 'Open website')}<br>"
        f"<b>Instagram:</b> {html_link(result.instagram, 'Open Instagram')}<br>"
        f"<b>Facebook:</b> {html_link(result.facebook, 'Open Facebook')}<br><br>"
        f"<b>TikTok:</b> {html_link(result.tiktok, 'Open TikTok')}<br><br>"
        f"<b>Menu online:</b> {bool_to_yes_no(result.menu_present)}<br>"
        f"<b>Booking online:</b> {bool_to_yes_no(result.booking_present)}<br>"
        f"<b>Delivery:</b> {bool_to_yes_no(result.delivery_present)}<br>"
        f"<b>Data capture:</b> {bool_to_yes_no(result.data_capture_present)}<br>"
        f"<b>Contact info:</b> {bool_to_yes_no(result.contact_present)}<br>"
        f"<b>Source:</b> {result.source or 'N/A'}"
    )


def safe_text(value) -> str:
    if value is None:
        return ""
    return str(value).replace("\r", " ").replace("\n", " ").strip()


def chunk_long_text(text: str, chunk_size: int = 90) -> str:
    text = safe_text(text)
    if not text:
        return ""

    parts = []
    while len(text) > chunk_size:
        parts.append(text[:chunk_size])
        text = text[chunk_size:]
    if text:
        parts.append(text)

    return "\n".join(parts)


def make_pdf_for_result(record_id: str, name: str, city: str, country: str, result) -> str:
    status = "ok"
    evidence_text = safe_text(result.evidence or "")

    if result.needs_review:
        status = "needs_review"
    if (
        ("timeout" in evidence_text.lower()) or
        ("request error" in evidence_text.lower())
    ) and not (result.website or result.instagram or result.facebook):
        status = "error"

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.set_margins(left=15, top=15, right=15)
    pdf.add_page()

    usable_width = pdf.w - pdf.l_margin - pdf.r_margin

    pdf.set_font("helvetica", "B", 14)
    pdf.multi_cell(usable_width, 10, "Online Presence Analysis")
    pdf.ln(2)

    pdf.set_font("helvetica", "", 11)

    lines = [
        f"HubSpot Record ID: {safe_text(record_id)}",
        f"Restaurant: {safe_text(name)}",
        f"City: {safe_text(city)}",
        f"Country: {safe_text(country)}",
        "",
        f"Website: {chunk_long_text(result.website or 'Not found')}",
        f"Instagram: {chunk_long_text(result.instagram or 'Not found')}",
        f"Facebook: {chunk_long_text(result.facebook or 'Not found')}",
        f"TikTok: {chunk_long_text(result.tiktok or 'Not found')}",
        "",
        f"Menu online: {bool_to_yes_no(result.menu_present)}",
        f"Booking online: {bool_to_yes_no(result.booking_present)}",
        f"Delivery: {bool_to_yes_no(result.delivery_present)}",
        f"Data capture: {bool_to_yes_no(result.data_capture_present)}",
        f"Contact info: {bool_to_yes_no(result.contact_present)}",
        "",
        f"Confidence: {round(result.confidence, 3)}",
        f"Source: {safe_text(result.source or 'N/A')}",
        f"Status: {status}",
        f"Needs review: {bool_to_yes_no(result.needs_review)}",
        f"Restaurant match: {bool_to_yes_no(result.is_restaurant_match)}",
        f"Non-restaurant reason: {safe_text(result.non_restaurant_reason or 'N/A')}",
        "",
        f"Evidence: {chunk_long_text(evidence_text, 85) or 'N/A'}",
        "",
        f"Generated at: {datetime.now(timezone.utc).isoformat()}",
    ]

    for line in lines:
        pdf.multi_cell(usable_width, 8, safe_text(line))
        pdf.ln(1)

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    tmp_path = tmp.name
    tmp.close()

    pdf.output(tmp_path)
    return tmp_path


def hubspot_upload_file(file_path: str, file_name: str) -> Optional[str]:
    url = f"{BASE_URL}/files/v3/files"

    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}"
    }

    options = '{"access":"PRIVATE"}'

    with open(file_path, "rb") as f:
        files = {
            "file": (file_name, f, "application/pdf")
        }
        data = {
            "fileName": file_name,
            "folderPath": "/online-presence-reports",
            "options": options
        }

        r = requests.post(url, headers=headers, files=files, data=data, timeout=60)

    if not r.ok:
        print("HubSpot file upload error:")
        print(r.status_code)
        print(r.text)

    r.raise_for_status()
    uploaded = r.json()
    return uploaded.get("id")


def build_missing_requirements_note(name: str, city: str, country: str) -> str:
    return (
        f"<b>Online Presence Analysis</b><br><br>"
        f"<b>Status:</b> no_requirements<br>"
        f"<b>Needs review:</b> Yes<br>"
        f"<b>Reason:</b> Missing required fields for analysis<br><br>"
        f"<b>Name:</b> {name or 'Missing'}<br>"
        f"<b>City:</b> {city or 'Missing'}<br>"
        f"<b>Country:</b> {country or 'Missing'}"
    )


def process_one_company(company: Dict[str, Any], ws, existing_ids: set[str]) -> None:
    record_id = company["id"]
    props = company.get("properties", {}) or {}

    name_company = (props.get("company") or "").strip()
    firstname = (props.get("firstname") or "").strip()
    lastname = (props.get("lastname") or "").strip()

    contact_name = " ".join(part for part in [firstname, lastname] if part).strip()

    if name_company and contact_name:
        name = f"{name_company} {contact_name}"
    elif name_company:
        name = name_company
    else:
        name = contact_name

    city = (props.get(PROP_CITY) or "").strip()
    country = (props.get(PROP_COUNTRY) or "Italy").strip()

    print("DEBUG record:", record_id)
    print("DEBUG props:", props)
    print("DEBUG company field:", name_company)
    print("DEBUG firstname field:", firstname)
    print("DEBUG lastname field:", lastname)
    print("DEBUG contact_name:", contact_name)
    print("DEBUG final name:", name)
    print("DEBUG city:", city)
    print("DEBUG country:", country)

    if already_processed(existing_ids, str(record_id)):
        print(f"Skipping {record_id}: already processed in Google Sheet")
        return

    if not name or not city:
        print(f"Record {record_id}: missing requirements, creating simple note and sheet row")

        note_body = build_missing_requirements_note(name, city, country)

        upsert_company_result(
            ws,
            company_id=str(record_id),
            name=name,
            city=city,
            country=country,
            result=None,
            status_override="no_requirements",
            needs_review_override="true",
            evidence_override="Missing required fields: name and/or city"
        )

        existing_ids.add(str(record_id))

        note_id = hubspot_create_note_for_contact(record_id, note_body, attachment_ids=[])
        print(f"Created simple note {note_id} for record {record_id} without PDF")
        return

    result = resolve_one(name, city, country)

    note_body = build_note_body(result, name, city, country)

    pdf_path = make_pdf_for_result(
        record_id=str(record_id),
        name=name,
        city=city,
        country=country,
        result=result
    )

    safe_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in name)
    pdf_file_name = f"{safe_name}_{record_id}_online_presence.pdf"

    file_id = ""
    pdf_url = ""

    try:
        file_id = hubspot_upload_file(pdf_path, pdf_file_name) or ""
        pdf_url = hubspot_get_signed_file_url(file_id) if file_id else ""
    except Exception as e:
        print(f"PDF upload skipped for record {record_id}: {e}")

    upsert_company_result(
        ws,
        company_id=str(record_id),
        name=name,
        city=city,
        country=country,
        result=result,
        hubspot_file_id=file_id,
        pdf_url=pdf_url or ""
    )

    existing_ids.add(str(record_id))

    attachment_ids = [file_id] if file_id else []
    note_id = hubspot_create_note_for_contact(
        record_id,
        note_body,
        attachment_ids=attachment_ids
    )

    print(f"Created note {note_id} for record {record_id} with PDF attachment {file_id}")
    print(f"PDF URL saved to Google Sheet: {pdf_url}")


def run_once(limit: int = POLL_LIMIT) -> None:
    ws = get_worksheet()
    existing_ids = get_existing_company_ids(ws)

    contacts = hubspot_list_contacts(limit=limit)
    print(f"Found {len(contacts)} records to process.")

    for contact in contacts:
        try:
            print(f"Processing record {contact['id']}...")
            process_one_company(contact, ws, existing_ids)
        except Exception as e:
            print(f"Error processing record {contact.get('id')}: {e}")
            traceback.print_exc()
        time.sleep(SLEEP_BETWEEN_RECORDS)


if __name__ == "__main__":
    run_once()
