import io
import json
import os
import re
import logging
import pdfplumber
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Sequence
from pdfminer.pdfdocument import PDFPasswordIncorrect


logger = logging.getLogger(__name__)
DEBUG_OUTPUT_DIR_ENV = "DEBUG_OUTPUT_DIR"
DEFAULT_DEBUG_LOG_DIR = Path(__file__).resolve().parent / "debug_logs"
CATEGORY_RULES_PATH = Path(__file__).resolve().parent / "category_rules.json"

TXN_PATTERN = re.compile(
    r"([A-Z][a-z]{2,8}\s+\d{2},\s+\d{4})\s+(.*?)\s+(DEBIT|CREDIT)\b",
    re.IGNORECASE
)

NORMALIZE_TEXT_PATTERN = re.compile(r"[^a-z0-9]+")
AMOUNT_ON_TYPE_LINE_PATTERN = re.compile(
    r"\b(?:DEBIT|CREDIT)\b[^\S\r\n]*(?:₹|INR)?[^\S\r\n]*([\d,]+(?:\.\d{1,2})?)\b",
    re.IGNORECASE,
)
AMOUNT_ON_TRANSACTION_ID_LINE_PATTERN = re.compile(
    r"\bTransaction ID\b.*?\b([\d,]+(?:\.\d{1,2})?)\b",
    re.IGNORECASE,
)
CURRENCY_AMOUNT_PATTERN = re.compile(
    r"(?:₹|INR)[^\S\r\n]*([\d,]+(?:\.\d{1,2})?)\b",
    re.IGNORECASE,
)
DECIMAL_AMOUNT_PATTERN = re.compile(r"(?<![\d:])([\d,]+\.\d{1,2})(?!\d)")
TIME_PATTERN = re.compile(r"\b(\d{1,2}:\d{2}\s*(?:AM|PM))\b", re.IGNORECASE)
UPI_HANDLE_PATTERN = re.compile(
    r"\b[a-z0-9][a-z0-9._-]{1,}@(okaxis|okhdfcbank|oksbi|okicici|ybl|ibl|axl|apl|paytm)\b",
    re.IGNORECASE,
)
ENTITY_PREFIX_PATTERN = re.compile(
    r"(?:Paid to|Received from|Payment to|Transfer to|Mobile recharged|Paid - Mobile Recharge|Paid Mobile Recharge|Bill paid|Broadband/Landline Success)\s*(.*)",
    re.IGNORECASE,
)
ENTITY_SEPARATOR_PATTERN = re.compile(
    r"\b(?:Transaction ID|UTR No|Debited from|Credited to|Debit|Credit)\b",
    re.IGNORECASE,
)
GENERIC_ENTITY_LABELS = {
    "paid": "Merchant Payment",
    "payment received": "Payment Received",
    "refund received": "Refund Received",
    "paid voucher": "Voucher",
    "paid mobile recharge": "Mobile Recharge",
    "mobile recharged": "Mobile Recharge",
    "bill paid": "Bill Payment",
    "broadband landline success": "Broadband/Landline Bill",
}

def load_category_rules() -> List[tuple[str, Sequence[str]]]:
    with CATEGORY_RULES_PATH.open(encoding="utf-8") as config_file:
        raw_rules = json.load(config_file)

    return [
        (entry["category"], tuple(entry["keywords"]))
        for entry in raw_rules
    ]

CATEGORY_RULES = load_category_rules()

def normalize_transaction_text(text: str) -> str:
    return NORMALIZE_TEXT_PATTERN.sub(" ", text.lower()).strip()

def contains_any_keyword(text: str, keywords: Sequence[str]) -> bool:
    padded_text = f" {text} "

    for keyword in keywords:
        normalized_keyword = normalize_transaction_text(keyword)
        if normalized_keyword and f" {normalized_keyword} " in padded_text:
            return True

    return False

def extract_amount(block_text: str) -> float:
    lines = [line.strip() for line in block_text.splitlines() if line.strip()]

    for line in lines:
        match = AMOUNT_ON_TYPE_LINE_PATTERN.search(line)
        if match:
            return float(match.group(1).replace(",", ""))

    for line in lines:
        if "transaction id" not in line.lower():
            continue

        match = AMOUNT_ON_TRANSACTION_ID_LINE_PATTERN.search(line)
        if match:
            return float(match.group(1).replace(",", ""))

    for line in lines:
        match = CURRENCY_AMOUNT_PATTERN.search(line)
        if match:
            return float(match.group(1).replace(",", ""))

    amount_matches = DECIMAL_AMOUNT_PATTERN.findall(block_text)
    if amount_matches:
        return float(amount_matches[-1].replace(",", ""))

    raise ValueError(f"Could not extract amount from transaction block: {block_text!r}")

def extract_time(block_text: str) -> str | None:
    match = TIME_PATTERN.search(block_text)
    return match.group(1).upper() if match else None

def normalize_generic_entity(label: str) -> str:
    normalized_label = normalize_transaction_text(label)
    return GENERIC_ENTITY_LABELS.get(normalized_label, label.strip())

def clean_entity_text(text: str) -> str:
    entity = ENTITY_SEPARATOR_PATTERN.split(text, maxsplit=1)[0]
    return entity.strip(" -:\n")

def categorize(details: str) -> str:
    normalized_details = normalize_transaction_text(details)

    if not normalized_details:
        return "P2P/Transfers"

    for category, keywords in CATEGORY_RULES:
        if contains_any_keyword(normalized_details, keywords):
            return category

    if UPI_HANDLE_PATTERN.search(details):
        return "P2P/Transfers"

    if "received from" in normalized_details or "paid to" in normalized_details:
        return "P2P/Transfers"

    return "Others"

def extract_utr(block_text: str) -> str | None:

    pattern = re.compile(r"UTR No\s*[\.\:]*\s*([a-zA-Z0-9]+)", re.IGNORECASE)
    match = pattern.search(block_text)

    return match.group(1).strip() if match else None

def extract_entity(block_text: str, fallback_details: str) -> str | None:
    match = ENTITY_PREFIX_PATTERN.search(block_text)

    if match:
        entity = clean_entity_text(match.group(1))
        if entity:
            return entity

    cleaned_fallback = clean_entity_text(fallback_details)
    if cleaned_fallback:
        return normalize_generic_entity(cleaned_fallback)

    return "Unknown"

def parse_statement_date(date_str: str, time_str: str | None = None) -> str:
    normalized_date_str = re.sub(r"\s+", " ", date_str).strip()
    normalized_date_str = re.sub(r"^Sept\b", "Sep", normalized_date_str, flags=re.IGNORECASE)

    datetime_formats = (
        ("%b %d, %Y", "%I:%M %p"),
        ("%B %d, %Y", "%I:%M %p"),
    )

    for date_format, time_format in datetime_formats:
        try:
            parsed_date = datetime.strptime(normalized_date_str, date_format)
            if not time_str:
                return parsed_date.strftime("%Y-%m-%d")

            parsed_datetime = datetime.strptime(
                f"{normalized_date_str} {time_str}",
                f"{date_format} {time_format}",
            )
            return parsed_datetime.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue

    raise ValueError(f"Unsupported statement date: {date_str}")

def get_debug_log_dir() -> Path:
    configured_dir = os.getenv(DEBUG_OUTPUT_DIR_ENV)
    if configured_dir:
        return Path(configured_dir).expanduser()

    return DEFAULT_DEBUG_LOG_DIR

def write_page_text_debug_log(
    job_id: str,
    page_text_data: List[tuple[int, str | None]],
    error_message: str | None = None,
) -> Path:
    debug_log_dir = get_debug_log_dir()
    debug_log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    log_path = debug_log_dir / f"{job_id}-{timestamp}.log"

    sections = [
        f"Job ID: {job_id}",
        f"Generated At: {timestamp}",
    ]

    if error_message:
        sections.extend([
            "Status: EXTRACTION_FAILED",
            f"Error: {error_message}",
        ])
    else:
        sections.append("Status: EXTRACTION_CAPTURED")

    if not page_text_data:
        sections.extend([
            "----",
            "No pages were available for text extraction.",
        ])
    else:
        for page_number, page_text in page_text_data:
            sections.extend([
                "----",
                f"Page {page_number}",
                page_text if page_text else "[No text extracted]",
            ])

    log_path.write_text("\n".join(sections) + "\n", encoding="utf-8")
    logger.info(f"Created debug extraction log at {log_path}")
    return log_path


def parse_transactions_from_text(full_text: str) -> Dict[str, Any]:
    if not full_text.strip():
        raise ValueError("No text could be extracted from the PDF. It might be a scanned image.")

    parsed_transactions = []
    matches = list(TXN_PATTERN.finditer(full_text))

    for i, match in enumerate(matches):
        date_str, details, txn_type = match.groups()

        start_idx = match.start()
        end_idx = matches[i + 1].start() if i + 1 < len(matches) else len(full_text)
        transaction_block = full_text[start_idx:end_idx]

        block_clean = " ".join(transaction_block.split())
        time_str = extract_time(block_clean)

        try:
            iso_date = parse_statement_date(date_str, time_str)
        except ValueError:
            logger.warning(f"Failed to parse date string: {date_str}. Using raw.")
            iso_date = date_str

        amount_clean = extract_amount(transaction_block)
        entity = extract_entity(block_clean, details)
        category = categorize(block_clean)
        utr_number = extract_utr(block_clean)

        parsed_transactions.append({
            "date": iso_date,
            "entity": entity,
            "amount": amount_clean,
            "type": txn_type.upper(),
            "category": category,
            "utr_number": utr_number,
            "raw_details": block_clean
        })

    if not parsed_transactions:
        return {
            "transactions": [],
            "summary": {
                "total_transactions": 0,
                "total_debit": 0.0,
                "total_credit": 0.0
            }
        }

    df = pd.DataFrame(parsed_transactions)
    total_txns = len(df)
    total_debit = df[df["type"] == "DEBIT"]["amount"].sum()
    total_credit = df[df["type"] == "CREDIT"]["amount"].sum()

    summary = {
        "total_transactions": int(total_txns),
        "total_debit": round(float(total_debit), 2),
        "total_credit": round(float(total_credit), 2)
    }

    return {
        "transactions": parsed_transactions,
        "summary": summary
    }


def parse_debug_log_content(log_text: str) -> Dict[str, Any]:
    page_sections = re.findall(r"Page\s+\d+\n(.*?)(?=\n----\nPage\s+\d+\n|\Z)", log_text, flags=re.DOTALL)
    full_text = "\n".join(section.strip() for section in page_sections if section.strip())
    return parse_transactions_from_text(full_text)

def parse_pdf_content(file_bytes: bytes, password: str = None, job_id: str | None = None) -> Dict[str, Any]:
    text_data: List[str] = []
    page_text_data: List[tuple[int, str | None]] = []

    try:
        with pdfplumber.open(io.BytesIO(file_bytes), password=password) as pdf:
            for page_number, page in enumerate(pdf.pages, start=1):
                page_text = page.extract_text(layout=True)
                page_text_data.append((page_number, page_text))
                if page_text:
                    text_data.append(page_text)

    except PDFPasswordIncorrect:
        logger.error("Failed to decrypt PDF: Incorrect password.")
        if job_id:
            write_page_text_debug_log(
                job_id,
                page_text_data,
                error_message="Incorrect password or unsupported PDF password.",
            )
        raise ValueError("The provided mobile number (password) is incorrect, or the PDF is locked with a different password.")
    except Exception as e:
        logger.error(f"Failed to read PDF file: {e}")
        if job_id:
            write_page_text_debug_log(job_id, page_text_data, error_message=str(e))
        raise ValueError("Invalid or corrupted PDF file.")

    if job_id:
        write_page_text_debug_log(job_id, page_text_data)

    full_text = "\n".join(text_data)
    return parse_transactions_from_text(full_text)
