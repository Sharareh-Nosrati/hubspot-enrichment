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

# Exact Excel schema order
RESULT_FIELDS = [
    "hubspot_company_id",
    "name",
    "city",
    "country",
    "website",
    "instagram",
    "facebook",
    "tiktok",
    "threads",
    "x",
    "youtube",
    "linktree",
    "uqrto",
    "google_maps_url",
    "justeat_url",
    "deliveroo_url",
    "thefork_url",
    "tripadvisor_url",
    "glovo_url",
    "restaurantguru_url",
    "opentable_url",
    "quandoo_url",
    "google_reviews_count",
    "google_rating_average",
    "instagram_bio_website",
    "website_creator",
    "website_creator_type",
    "website_creator_confidence",
    "website_creator_source",
    "website_platform",
    "website_score",
    "website_validated",
    "website_validation_score",
    "website_validation_reason",
    "instagram_score",
    "facebook_score",
    "tiktok_score",
    "threads_score",
    "x_score",
    "youtube_score",
    "linktree_score",
    "uqrto_score",
    "website_match_reason",
    "instagram_match_reason",
    "facebook_match_reason",
    "tiktok_match_reason",
    "threads_match_reason",
    "x_match_reason",
    "youtube_match_reason",
    "linktree_match_reason",
    "uqrto_match_reason",
    "website_found_from",
    "instagram_found_from",
    "facebook_found_from",
    "tiktok_found_from",
    "threads_found_from",
    "x_found_from",
    "youtube_found_from",
    "linktree_found_from",
    "uqrto_found_from",
    "instagram_bio_links_json",
    "facebook_bio_links_json",
    "instagram_primary_external_link",
    "facebook_primary_external_link",
    "directory_links_json",
    "official_website_candidates_json",
    "has_directory_profile",
    "has_google_maps",
    "has_justeat",
    "has_deliveroo",
    "has_thefork",
    "has_tripadvisor",
    "has_glovo",
    "has_restaurantguru",
    "has_opentable",
    "has_quandoo",
    "confidence",
    "source",
    "evidence",
    "needs_review",
    "status",
    "menu_present",
    "booking_present",
    "delivery_present",
    "data_capture_present",
    "contact_present",
    "is_restaurant_match",
    "non_restaurant_reason",
    "tiktok_present",
    "last_checked",
    "hubspot_file_id",
    "pdf_url",
]

HEADERS = RESULT_FIELDS[:]


def hs_headers() -> Dict[str, str]:
    if not HUBSPOT_TOKEN:
        raise ValueError("HUBSPOT_TOKEN is missing.")
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json"
    }


def get_attr(result: Any, key: str, default: Any = "") -> Any:
    if result is None:
        return default
    return getattr(result, key, default)


def normalize_value(key: str, value: Any) -> str:
    bool_fields = {
        "website_validated",
        "has_directory_profile",
        "has_google_maps",
        "has_justeat",
        "has_deliveroo",
        "has_thefork",
        "has_tripadvisor",
        "has_glovo",
        "has_restaurantguru",
        "has_opentable",
        "has_quandoo",
        "needs_review",
        "menu_present",
        "booking_present",
        "delivery_present",
        "data_capture_present",
        "contact_present",
        "is_restaurant_match",
        "tiktok_present",
    }

    float_fields = {
        "website_score",
        "website_validation_score",
        "instagram_score",
        "facebook_score",
        "tiktok_score",
        "threads_score",
        "x_score",
        "youtube_score",
        "linktree_score",
        "uqrto_score",
        "confidence",
    }

    json_fields = {
        "instagram_bio_links_json",
        "facebook_bio_links_json",
        "directory_links_json",
        "official_website_candidates_json",
    }

    if value is None:
        return ""

    if key in bool_fields:
        return str(bool(value)).lower()

    if key in float_fields:
        try:
            return str(round(float(value), 3))
        except Exception:
            return ""

    if key in json_fields:
        if isinstance(value, str):
            return value
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)

    return str(value)


def excel_col_letter(col_num: int) -> str:
    result = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result


def header_range() -> str:
    return f"A1:{excel_col_letter(len(HEADERS))}1"


def row_range(row_number: int) -> str:
    return f"A{row_number}:{excel_col_letter(len(HEADERS))}{row_number}"


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

    creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
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
        ws.update(values=[HEADERS], range_name=header_range())

    _worksheet = ws
    return _worksheet


def get_existing_company_ids(ws) -> set[str]:
    values = ws.col_values(1)
    return {v for v in values[1:] if v}


def find_row_by_company_id(ws, company_id: str):
    values = ws.get_all_values()
    for idx, row in enumerate(values[1:], start=2):
        if row and row[0] == str(company_id):
            return idx
    return None


def already_processed(existing_ids: set[str], company_id: str) -> bool:
    return str(company_id) in existing_ids


def compute_status(result) -> str:
    if result is None:
        return "no_requirements"

    status = "ok"
    evidence_text = get_attr(result, "evidence", "") or ""

    if get_attr(result, "needs_review", False):
        status = "needs_review"

    if (
        ("timeout" in evidence_text.lower()) or
        ("request error" in evidence_text.lower())
    ) and not (
        get_attr(result, "website", "") or
        get_attr(result, "instagram", "") or
        get_attr(result, "facebook", "") or
        get_attr(result, "tiktok", "")
    ):
        status = "error"

    return status


def hubspot_get_signed_file_url(file_id: str) -> Optional[str]:
    url = f"{BASE_URL}/files/v3/files/{file_id}/signed-url"
    r = requests.get(url, headers=hs_headers(), timeout=30)

    if not r.ok:
        print("HubSpot signed URL error:")
        print(r.status_code)
        print(r.text)
        return None

    return r.json().get("url")


def build_row(
    company_id: str,
    name: str,
    city: str,
    country: str,
    result=None,
    hubspot_file_id: str = "",
    pdf_url: str = "",
    status_override: str = "",
    needs_review_override: str = "",
    evidence_override: str = "",
) -> List[str]:
    row_data = {key: "" for key in RESULT_FIELDS}

    row_data["hubspot_company_id"] = str(company_id)
    row_data["name"] = name
    row_data["city"] = city
    row_data["country"] = country
    row_data["last_checked"] = datetime.now(timezone.utc).isoformat()
    row_data["hubspot_file_id"] = hubspot_file_id or ""
    row_data["pdf_url"] = pdf_url or ""

    if result is None:
        row_data["status"] = status_override or "no_requirements"
        row_data["needs_review"] = needs_review_override or "true"
        row_data["evidence"] = evidence_override or "Missing required fields: name and/or city"
        row_data["is_restaurant_match"] = "false"
        row_data["non_restaurant_reason"] = "Missing required fields: name and/or city"
        row_data["tiktok_present"] = "false"
        return [normalize_value(key, row_data[key]) for key in RESULT_FIELDS]

    for key in RESULT_FIELDS:
        if key in {
            "hubspot_company_id", "name", "city", "country",
            "last_checked", "hubspot_file_id", "pdf_url", "status"
        }:
            continue
        row_data[key] = get_attr(result, key, "")

    row_data["status"] = status_override or compute_status(result)

    if needs_review_override:
        row_data["needs_review"] = needs_review_override
    if evidence_override:
        row_data["evidence"] = evidence_override

    row_data["last_checked"] = datetime.now(timezone.utc).isoformat()
    row_data["hubspot_file_id"] = hubspot_file_id or ""
    row_data["pdf_url"] = pdf_url or ""

    return [normalize_value(key, row_data[key]) for key in RESULT_FIELDS]


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
    row = build_row(
        company_id=company_id,
        name=name,
        city=city,
        country=country,
        result=result,
        hubspot_file_id=hubspot_file_id,
        pdf_url=pdf_url,
        status_override=status_override,
        needs_review_override=needs_review_override,
        evidence_override=evidence_override,
    )

    existing_row = find_row_by_company_id(ws, str(company_id))
    if existing_row:
        ws.update(
            values=[row],
            range_name=row_range(existing_row),
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
        "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}]
    }

    r = requests.post(url, headers=hs_headers(), json=payload, timeout=30)

    if not r.ok:
        print("HubSpot contact search error:")
        print(r.status_code)
        print(r.text)

    r.raise_for_status()
    return r.json().get("results", [])


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
    return r.json().get("id")


def bool_to_yes_no(value: bool) -> str:
    return "Yes" if value else "No"


def html_link(url: str, label: str) -> str:
    if not url:
        return "Not found"
    return f'<a href="{url}" target="_blank" rel="noopener noreferrer">{label}</a>'


def build_note_body(result, name: str, city: str, country: str) -> str:
    status = compute_status(result)

    return (
        f"<b>Online Presence Analysis</b><br><br>"
        f"<b>Lead:</b> {name or 'N/A'}<br>"
        f"<b>City:</b> {city or 'N/A'}<br>"
        f"<b>Country:</b> {country or 'N/A'}<br>"
        f"<b>Confidence:</b> {normalize_value('confidence', get_attr(result, 'confidence', ''))}<br>"
        f"<b>Status:</b> {status}<br>"
        f"<b>Needs review:</b> {bool_to_yes_no(get_attr(result, 'needs_review', False))}<br>"
        f"<b>Website:</b> {html_link(get_attr(result, 'website', ''), 'Open website')}<br>"
        f"<b>Instagram:</b> {html_link(get_attr(result, 'instagram', ''), 'Open Instagram')}<br>"
        f"<b>Facebook:</b> {html_link(get_attr(result, 'facebook', ''), 'Open Facebook')}<br>"
        f"<b>TikTok:</b> {html_link(get_attr(result, 'tiktok', ''), 'Open TikTok')}<br>"
        f"<b>Google Maps:</b> {html_link(get_attr(result, 'google_maps_url', ''), 'Open Google Maps')}<br>"
        f"<b>Menu:</b> {bool_to_yes_no(get_attr(result, 'menu_present', False))}<br>"
        f"<b>Booking:</b> {bool_to_yes_no(get_attr(result, 'booking_present', False))}<br>"
        f"<b>Delivery:</b> {bool_to_yes_no(get_attr(result, 'delivery_present', False))}<br>"
        f"<b>Data capture:</b> {bool_to_yes_no(get_attr(result, 'data_capture_present', False))}<br>"
        f"<b>Contact info:</b> {bool_to_yes_no(get_attr(result, 'contact_present', False))}<br>"
        f"<b>Website creator:</b> {get_attr(result, 'website_creator', '') or 'N/A'}<br>"
        f"<b>Source:</b> {get_attr(result, 'source', '') or 'N/A'}"
    )


def safe_text(value) -> str:
    if value is None:
        return ""

    text = str(value)
    replacements = {
        "→": "->",
        "←": "<-",
        "•": "-",
        "–": "-",
        "—": "-",
        "“": '"',
        "”": '"',
        "‘": "'",
        "’": "'",
        "…": "...",
        "\u00a0": " ",
    }

    for bad, good in replacements.items():
        text = text.replace(bad, good)

    text = text.replace("\r", " ").replace("\n", " ").strip()
    return text.encode("latin-1", errors="replace").decode("latin-1")


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
        f"Lead: {safe_text(name)}",
        f"City: {safe_text(city)}",
        f"Country: {safe_text(country)}",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        f"Website: {chunk_long_text(get_attr(result, 'website', '') or 'Not found')}",
        f"Instagram: {chunk_long_text(get_attr(result, 'instagram', '') or 'Not found')}",
        f"Facebook: {chunk_long_text(get_attr(result, 'facebook', '') or 'Not found')}",
        f"TikTok: {chunk_long_text(get_attr(result, 'tiktok', '') or 'Not found')}",
        f"Threads: {chunk_long_text(get_attr(result, 'threads', '') or 'Not found')}",
        f"X: {chunk_long_text(get_attr(result, 'x', '') or 'Not found')}",
        f"YouTube: {chunk_long_text(get_attr(result, 'youtube', '') or 'Not found')}",
        f"Google Maps: {chunk_long_text(get_attr(result, 'google_maps_url', '') or 'Not found')}",
        "",
        f"Menu: {bool_to_yes_no(get_attr(result, 'menu_present', False))}",
        f"Booking: {bool_to_yes_no(get_attr(result, 'booking_present', False))}",
        f"Delivery: {bool_to_yes_no(get_attr(result, 'delivery_present', False))}",
        f"Data capture: {bool_to_yes_no(get_attr(result, 'data_capture_present', False))}",
        f"Contact info: {bool_to_yes_no(get_attr(result, 'contact_present', False))}",
        f"Website creator: {safe_text(get_attr(result, 'website_creator', '') or 'N/A')}",
        "",
        f"Confidence: {normalize_value('confidence', get_attr(result, 'confidence', ''))}",
        f"Source: {safe_text(get_attr(result, 'source', '') or 'N/A')}",
        f"Evidence: {chunk_long_text(get_attr(result, 'evidence', '') or 'N/A', 85)}",
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
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
    options = '{"access":"PRIVATE"}'

    with open(file_path, "rb") as f:
        files = {"file": (file_name, f, "application/pdf")}
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
    return r.json().get("id")


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
    print("DEBUG final name:", name)
    print("DEBUG city:", city)
    print("DEBUG country:", country)

    if already_processed(existing_ids, str(record_id)):
        print(f"Skipping {record_id}: already processed in Google Sheet")
        return

    if not name or not city:
        note_body = build_missing_requirements_note(name, city, country)

        upsert_company_result(
            ws=ws,
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
        ws=ws,
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
    note_id = hubspot_create_note_for_contact(record_id, note_body, attachment_ids=attachment_ids)

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