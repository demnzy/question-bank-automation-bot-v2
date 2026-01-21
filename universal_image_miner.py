import sys
import json
import os
import re
import fitz  # PyMuPDF
import pandas as pd
import cloudscraper
import pytesseract
import time
from PIL import Image
from io import BytesIO

# --- CONFIGURATION ---
LOGIN_URL = "https://devbackend.succeedquiz.com/api/v1/auth/login"
UPLOAD_URL = "https://devbackend.succeedquiz.com/api/v1/upload"

scraper = cloudscraper.create_scraper()

# ----------------- AUTH & UPLOAD -----------------

def login_and_get_token():
    email = os.environ.get("SUCCEED_EMAIL")
    password = os.environ.get("SUCCEED_PASSWORD")
    if not email or not password: return None

    try:
        response = scraper.post(LOGIN_URL, json={"email": email, "password": password})
        if response.status_code in [200, 201]:
            return response.json().get('data', {}).get('accessToken')
        print(f"Login Failed: {response.status_code}")
        return None
    except: return None

def upload_image_api(image_bytes, filename, token):
    headers = {'Authorization': f'Bearer {token}'}
    if "." not in filename: filename += ".jpg"
    files = [('file', (filename, image_bytes, 'image/jpeg'))]

    try:
        response = scraper.post(UPLOAD_URL, headers=headers, files=files)
        if response.status_code in [200, 201]:
            data = response.json()
            if 'data' in data and 'files' in data['data']: return data['data']['files'][0].get('url')
            if 'url' in data: return data['url']
            if 'secure_url' in data: return data['secure_url']
        return None
    except: return None

# ----------------- COORDINATE CROPPER (SOURCE OF TRUTH) -----------------

def crop_image_from_coords(doc, page_num, bbox_str):
    """
    Precision Cropping using Cirrascale's BBOX (0-100 relative scale).
    """
    try:
        parts = [float(x.strip()) for x in bbox_str.split(',')]
        if len(parts) != 4: return None
        ymin, xmin, ymax, xmax = parts # Standard OLM output

        page_idx = int(page_num) - 1 
        if page_idx < 0 or page_idx >= len(doc): return None
        
        page = doc[page_idx]
        w, h = page.rect.width, page.rect.height

        clip_rect = fitz.Rect(
            (max(0, xmin)/100)*w, (max(0, ymin)/100)*h,
            (min(100, xmax)/100)*w, (min(100, ymax)/100)*h
        )
        # High DPI for clear Hotspots/Diagrams
        return page.get_pixmap(clip=clip_rect, dpi=200).tobytes("jpg")
    except: return None

# ----------------- CONSOLIDATION / VERIFICATION -----------------

def verify_and_rescue_text(image_bytes):
    """
    FALLBACK ONLY: If Cirrascale missed the text and gave an image instead,
    we use Tesseract to consolidate the data.
    """
    try:
        img = Image.open(BytesIO(image_bytes))
        text = pytesseract.image_to_string(img)
        # Format as semicolon list for the Options column
        clean = [l.strip() for l in text.split('\n') if l.strip()]
        return "; ".join(clean)
    except: return None

# ----------------- MAIN -----------------

def main():
    if len(sys.argv) < 5: return # Expects excel, pdf, coords, output

    input_excel = sys.argv[1]
    pdf_path = sys.argv[2]
    coord_path = sys.argv[3]
    output_json = sys.argv[4]

    token = login_and_get_token()
    if not token: return

    try:
        df = pd.read_excel(input_excel)
        doc = fitz.open(pdf_path)
        with open(coord_path, 'r', encoding='utf-8') as f:
            raw = json.load(f)
            coord_map = json.loads(raw) if isinstance(raw, str) else raw
    except: return

    result_map = {} 
    ref_pattern = re.compile(r"<<(IMAGE_REF_\d+)>>")

    print(f"Consolidating V2 Data for {len(df)} questions...")

    for idx, row in df.iterrows():
        q_text_raw = str(row.get('Question', ''))
        # Clean text for mapping (stripping tokens)
        q_text_clean = re.sub(r"<<IMAGE_REF_\d+>>", "", q_text_raw).strip()
        
        q_type = str(row.get('Question_Type', '')).lower()
        q_options = str(row.get('Options', ''))

        # Check for Image References (Source of Truth)
        matches = ref_pattern.findall(q_text_raw)
        
        if matches:
            for ref_id in matches:
                if ref_id in coord_map:
                    # 1. Retrieve Data from Source of Truth
                    meta = coord_map[ref_id]
                    img_bytes = crop_image_from_coords(doc, meta.get('page', 1), meta.get('coordinates', ''))
                    
                    if img_bytes:
                        # 2. Universal Upload (Hotspots, Exhibits, etc.)
                        url = upload_image_api(img_bytes, f"q{idx+1}_{ref_id}.jpg", token)
                        if url:
                            print(f"Q{idx+1}: Image Mapped -> {url}")
                            result_map[q_text_raw] = url
                            result_map[q_text_clean] = url

                        # 3. CONSOLIDATION CHECK (The "Verification" Step)
                        # Did Cirrascale treat a text box as an image by mistake?
                        is_drag_drop = "drag" in q_type or "drop" in q_type
                        is_empty_options = q_options == 'nan' or not q_options.strip()

                        if is_drag_drop and is_empty_options:
                            print(f"  [WARN] Q{idx+1}: Drag/Drop options missing! Cirrascale gave image instead of text.")
                            print(f"  -> Attempting Tesseract Rescue...")
                            
                            rescued_text = verify_and_rescue_text(img_bytes)
                            if rescued_text:
                                # Save with _OCR suffix for Universal Miner to pick up
                                result_map[q_text_raw + "_OCR"] = rescued_text
                                print(f"  -> Rescued Text: {rescued_text[:40]}...")
                            else:
                                print(f"  -> Rescue Failed. Question may need manual review.")

    # Save Final Map
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(result_map, f, indent=4)

    print(f"Consolidation Complete. Output: {output_json}")

if __name__ == "__main__":
    main()
