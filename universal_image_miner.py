import json
import logging
import os
import re
import sys
from io import BytesIO
import cloudscraper
import fitz  # PyMuPDF
import pandas as pd
from PIL import Image
import pytesseract

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

LOGIN_URL = "https://devbackend.succeedquiz.com/api/v1/auth/login"
UPLOAD_URL = "https://devbackend.succeedquiz.com/api/v1/upload"

scraper = cloudscraper.create_scraper()


def login_and_get_token():
    """Authenticate against the LMS API and return the access token."""
    email = os.environ.get("SUCCEED_EMAIL")
    password = os.environ.get("SUCCEED_PASSWORD")

    if not email or not password:
        logger.error("Missing required environment credentials: SUCCEED_EMAIL and SUCCEED_PASSWORD.")
        return None

    try:
        response = scraper.post(LOGIN_URL, json={"email": email, "password": password})
        if response.status_code in (200, 201):
            logger.info("Authentication successful.")
            return response.json().get("data", {}).get("accessToken")

        logger.error("Authentication failed: HTTP %s - %s", response.status_code, response.text)
        return None
    except Exception as e:
        logger.error("Authentication exception occurred: %s", e)
        return None


def upload_image_api(image_bytes, filename, token):
    """Upload cropped diagram image to cloud media storage."""
    headers = {"Authorization": f"Bearer {token}"}
    if "." not in filename:
        filename += ".jpg"
    files = [("file", (filename, image_bytes, "image/jpeg"))]

    try:
        response = scraper.post(UPLOAD_URL, headers=headers, files=files)
        if response.status_code in (200, 201):
            data = response.json()
            if "data" in data and "files" in data["data"]:
                return data["data"]["files"][0].get("url")
            if "url" in data:
                return data["url"]
            if "secure_url" in data:
                return data["secure_url"]
        logger.warning("Upload returned status %s: %s", response.status_code, response.text)
        return None
    except Exception as e:
        logger.warning("Image upload error: %s", e)
        return None


def crop_image_from_coords(doc, page_num, bbox_str):
    """
    Crops image from PDF page using bounding box percentages (ymin, xmin, ymax, xmax).
    """
    try:
        if not bbox_str or str(bbox_str).strip() in ("", "true", "none"):
            return None

        parts = [float(x.strip()) for x in str(bbox_str).split(",")]
        if len(parts) != 4:
            return None
        ymin, xmin, ymax, xmax = parts

        page_idx = int(page_num) - 1
        if page_idx < 0 or page_idx >= len(doc):
            return None

        page = doc[page_idx]
        w, h = page.rect.width, page.rect.height

        clip_rect = fitz.Rect(
            (max(0.0, xmin) / 100.0) * w,
            (max(0.0, ymin) / 100.0) * h,
            (min(100.0, xmax) / 100.0) * w,
            (min(100.0, ymax) / 100.0) * h,
        )
        return page.get_pixmap(clip=clip_rect, dpi=200).tobytes("jpg")
    except Exception as e:
        logger.debug("Coordinate crop exception: %s", e)
        return None


def crop_image_via_text_anchoring(doc, q_text):
    """
    Fallback: Searches the PDF for question prompt text and crops the region directly beneath it.
    """
    if not q_text or len(q_text) < 15:
        return None

    clean = re.sub(r"<<.*?>>", "", q_text).strip()
    search_targets = [clean[:60], clean[:30]]

    for target in search_targets:
        target = target.strip()
        if len(target) < 10:
            continue

        for page_idx in range(len(doc)):
            page = doc[page_idx]
            rects = page.search_for(target)

            if rects:
                bottom_y = max(r.y1 for r in rects)
                w, h = page.rect.width, page.rect.height

                y0 = min(bottom_y + 10, h)
                y1 = min(y0 + 400, h)

                clip_rect = fitz.Rect(0, y0, w, y1)
                try:
                    return page.get_pixmap(clip=clip_rect, dpi=200).tobytes("jpg")
                except Exception as e:
                    logger.debug("Text anchor rendering failed: %s", e)
                    return None
    return None


def verify_and_rescue_text(image_bytes):
    """Fallback OCR extraction on image bytes for unparsed drag-and-drop choices."""
    try:
        img = Image.open(BytesIO(image_bytes))
        text = pytesseract.image_to_string(img)
        clean = [line.strip() for line in text.split("\n") if line.strip()]
        return "; ".join(clean)
    except Exception as e:
        logger.debug("OCR rescue error: %s", e)
        return None


def main():
    if len(sys.argv) < 5:
        logger.error("Usage: python universal_image_miner.py <input_excel> <input_pdf> <coords_json> <output_json>")
        sys.exit(1)

    input_excel = sys.argv[1]
    pdf_path = sys.argv[2]
    coord_path = sys.argv[3]
    output_json = sys.argv[4]

    token = login_and_get_token()
    if not token:
        logger.error("Exiting: Authentication token could not be obtained.")
        sys.exit(1)

    coord_map = {}
    try:
        df = pd.read_excel(input_excel)
        doc = fitz.open(pdf_path)

        if os.path.exists(coord_path):
            with open(coord_path, "r", encoding="utf-8") as f:
                file_content = f.read().strip()
                if file_content:
                    raw = json.loads(file_content)
                    coord_map = json.loads(raw) if isinstance(raw, str) else raw
                    logger.info("Loaded %d coordinate entries from %s", len(coord_map), coord_path)
                else:
                    logger.info("Coordinate map file is empty; initialized with empty mapping.")
    except Exception as e:
        logger.error("Failed to load source documents or coordinate file: %s", e)
        with open(output_json, "w", encoding="utf-8") as f:
            json.dump({}, f)
        sys.exit(1)

    result_map = {}
    ref_pattern = re.compile(r"<<(IMAGE_REF_\d+)>>")
    primary_success = 0
    anchor_rescues = 0
    failures = 0

    logger.info("Starting hybrid image extraction across %d rows...", len(df))

    for idx, row in df.iterrows():
        q_text_raw = str(row.get("Question", ""))
        q_text_clean = re.sub(r"<<IMAGE_REF_\d+>>", "", q_text_raw).strip()
        q_type = str(row.get("Question_Type", "")).lower()
        q_options = str(row.get("Options", ""))

        has_image_raw = str(row.get("has_image", "")).lower()
        is_has_image_true = has_image_raw in ("true", "1", "yes")

        matches = ref_pattern.findall(q_text_raw)
        img_bytes = None
        used_anchor = False
        ref_name = "AUTO"

        if matches:
            for ref_id in matches:
                ref_name = ref_id
                # Strategy 1: Bounding-box coordinate extraction
                if ref_id in coord_map and coord_map[ref_id].get("coordinates") not in ("", "true"):
                    meta = coord_map[ref_id]
                    img_bytes = crop_image_from_coords(doc, meta.get("page", 1), meta.get("coordinates"))

                # Strategy 2: Fallback to text anchoring if coordinates missing or invalid
                if not img_bytes:
                    logger.info("Row %d [%s]: Coordinates unavailable; falling back to text anchoring.", idx + 1, ref_id)
                    img_bytes = crop_image_via_text_anchoring(doc, q_text_clean)
                    used_anchor = True
        elif is_has_image_true:
            # Strategy 3: has_image is true without inline image tags
            logger.info("Row %d: 'has_image' set; using text anchor extraction.", idx + 1)
            img_bytes = crop_image_via_text_anchoring(doc, q_text_clean)
            used_anchor = True

        if img_bytes:
            url = upload_image_api(img_bytes, f"q{idx+1}_{ref_name}.jpg", token)
            if url:
                if used_anchor:
                    logger.info("Row %d: Extracted via text anchor -> %s", idx + 1, url)
                    anchor_rescues += 1
                else:
                    logger.info("Row %d: Extracted via bounding box -> %s", idx + 1, url)
                    primary_success += 1

                result_map[q_text_raw] = url
                result_map[q_text_clean] = url

                # Verify OCR for drag-and-drop questions missing option text
                if ("drag" in q_type or "drop" in q_type) and (q_options == "nan" or not q_options.strip()):
                    rescued_text = verify_and_rescue_text(img_bytes)
                    if rescued_text:
                        result_map[q_text_raw + "_OCR"] = rescued_text
            else:
                logger.warning("Row %d: Image cropped but upload failed.", idx + 1)
                failures += 1
        elif matches or is_has_image_true:
            logger.warning("Row %d: Extraction failed across all strategies.", idx + 1)
            failures += 1

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(result_map, f, indent=2)

    logger.info("Extraction summary: %d bounding-box crops, %d anchor rescues, %d failures.",
                primary_success, anchor_rescues, failures)


if __name__ == "__main__":
    main()
