import sys
import json
import os
import re
import fitz  # PyMuPDF
import pandas as pd
import cloudscraper
import pytesseract
from PIL import Image
from io import BytesIO

# --- CONFIGURATION ---
LOGIN_URL = "https://devbackend.succeedquiz.com/api/v1/auth/login"
UPLOAD_URL = "https://devbackend.succeedquiz.com/api/v1/upload"

scraper = cloudscraper.create_scraper()

# ----------------- AUTH & UPLOAD -----------------

def login_and_get_token():
    email = "odavies@readwriteds.com"
    password = "2862008June28?"
    
    if not email or not password: 
        print("Missing hardcoded email or password.")
        return None

    try:
        response = scraper.post(LOGIN_URL, json={"email": email, "password": password})
        if response.status_code in [200, 201]:
            print("✅ Login Successful!")
            return response.json().get('data', {}).get('accessToken')
            
        print(f"❌ Login Failed: {response.status_code} - {response.text}")
        return None
    except Exception as e:
        print(f"❌ Login Exception occurred: {e}")
        return None

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

# ----------------- IMAGE CROPPING ENGINES -----------------

def crop_image_from_coords(doc, page_num, bbox_str):
    """PRIMARY: Precision Cropping using Cirrascale's BBOX."""
    try:
        if not bbox_str or bbox_str.strip() == "" or bbox_str == "true":
            return None 
            
        parts = [float(x.strip()) for x in bbox_str.split(',')]
        if len(parts) != 4: return None
        ymin, xmin, ymax, xmax = parts 

        page_idx = int(page_num) - 1 
        if page_idx < 0 or page_idx >= len(doc): return None
        
        page = doc[page_idx]
        w, h = page.rect.width, page.rect.height

        clip_rect = fitz.Rect(
            (max(0, xmin)/100)*w, (max(0, ymin)/100)*h,
            (min(100, xmax)/100)*w, (min(100, ymax)/100)*h
        )
        return page.get_pixmap(clip=clip_rect, dpi=200).tobytes("jpg")
    except Exception as e: 
        print(f"    -> [CROP EXCEPTION]: {e}")
        return None

def crop_image_via_text_anchoring(doc, q_text):
    """FALLBACK: Searches PDF for the question text and crops the area below it."""
    if not q_text or len(q_text) < 15: return None
    
    # Clean text and try a long search string, then a shorter one if it fails
    clean = re.sub(r"<<.*?>>", "", q_text).strip()
    search_targets = [clean[:60], clean[:30]]

    for target in search_targets:
        target = target.strip()
        if len(target) < 10: continue
            
        for page_idx in range(len(doc)):
            page = doc[page_idx]
            rects = page.search_for(target)
            
            if rects:
                # Find the bottom-most Y coordinate of the found text
                bottom_y = max([r.y1 for r in rects])
                w, h = page.rect.width, page.rect.height
                
                # Crop from just below the text down to either 400pts or the page bottom
                y0 = min(bottom_y + 10, h) 
                y1 = min(y0 + 400, h)
                
                clip_rect = fitz.Rect(0, y0, w, y1)
                try:
                    return page.get_pixmap(clip=clip_rect, dpi=200).tobytes("jpg")
                except:
                    return None
    return None

# ----------------- OCR RESCUE -----------------

def verify_and_rescue_text(image_bytes):
    try:
        img = Image.open(BytesIO(image_bytes))
        text = pytesseract.image_to_string(img)
        clean = [l.strip() for l in text.split('\n') if l.strip()]
        return "; ".join(clean)
    except: return None

# ----------------- MAIN -----------------

def main():
    if len(sys.argv) < 5: 
        print("❌ Missing arguments. Expects: excel, pdf, coords, output")
        return 

    input_excel = sys.argv[1]
    pdf_path = sys.argv[2]
    coord_path = sys.argv[3]
    output_json = sys.argv[4]

    token = login_and_get_token()
    if not token: 
        print("❌ Exiting: Could not obtain auth token.")
        return

    # --- AGGRESSIVE ERROR HANDLING (Prevents Empty JSON Crash) ---
    try:
        df = pd.read_excel(input_excel)
        doc = fitz.open(pdf_path)
        
        with open(coord_path, 'r', encoding='utf-8') as f:
            file_content = f.read().strip()
            if not file_content:
                coord_map = {}
                print("⚠️ coords.json was empty. Defaulting to an empty map.")
            else:
                raw = json.loads(file_content)
                coord_map = json.loads(raw) if isinstance(raw, str) else raw
                print(f"✅ Loaded {len(coord_map)} entries from coords.json")
                
    except Exception as e:
        print(f"\n🚨 CRITICAL ERROR LOADING FILES 🚨\n{e}\n")
        with open(output_json, 'w', encoding='utf-8') as f: json.dump({}, f)
        return

    result_map = {} 
    ref_pattern = re.compile(r"<<(IMAGE_REF_\d+)>>")

    # Tracking Stats
    stats = {"primary_success": 0, "anchor_rescues": 0, "failures": 0}

    print(f"\n--- Starting V2 Hybrid Image Extraction for {len(df)} questions ---\n")

    for idx, row in df.iterrows():
        q_text_raw = str(row.get('Question', ''))
        q_text_clean = re.sub(r"<<IMAGE_REF_\d+>>", "", q_text_raw).strip()
        q_type = str(row.get('Question_Type', '')).lower()
        q_options = str(row.get('Options', ''))
        
        has_image_raw = str(row.get('has_image', '')).lower()
        is_has_image_true = has_image_raw in ['true', '1', 'yes']

        matches = ref_pattern.findall(q_text_raw)
        img_bytes = None
        used_anchor = False
        ref_name = "AUTO"

        if matches:
            for ref_id in matches:
                ref_name = ref_id
                # Strategy 1: CirraScale Coordinates
                if ref_id in coord_map and coord_map[ref_id].get('coordinates') not in ['', 'true']:
                    meta = coord_map[ref_id]
                    img_bytes = crop_image_from_coords(doc, meta.get('page', 1), meta.get('coordinates'))
                
                # Strategy 2: Fallback to Text Anchoring if coords missing/empty
                if not img_bytes:
                    print(f"⚠️ Q{idx+1} [{ref_id}]: Coords missing/invalid. Attempting Text Anchor Rescue...")
                    img_bytes = crop_image_via_text_anchoring(doc, q_text_clean)
                    used_anchor = True
        else:
            # Strategy 3: No tags, but 'has_image' is true
            if is_has_image_true:
                print(f"🔍 Q{idx+1}: 'has_image=True' but no tags. Attempting Text Anchor Rescue...")
                img_bytes = crop_image_via_text_anchoring(doc, q_text_clean)
                used_anchor = True

        # Upload Logic
        if img_bytes:
            url = upload_image_api(img_bytes, f"q{idx+1}_{ref_name}.jpg", token)
            if url:
                if used_anchor:
                    print(f"⚓ Q{idx+1}: RESCUED via Text Anchor -> {url}")
                    stats["anchor_rescues"] += 1
                else:
                    print(f"✅ Q{idx+1}: CirraScale Crop -> {url}")
                    stats["primary_success"] += 1
                    
                result_map[q_text_raw] = url
                result_map[q_text_clean] = url

                # Check if we need to OCR a drag/drop image
                if ("drag" in q_type or "drop" in q_type) and (q_options == 'nan' or not q_options.strip()):
                    rescued_text = verify_and_rescue_text(img_bytes)
                    if rescued_text: result_map[q_text_raw + "_OCR"] = rescued_text
            else:
                print(f"❌ Q{idx+1}: Cropped successfully, but UPLOAD FAILED.")
                stats["failures"] += 1
        elif matches or is_has_image_true:
            print(f"❌ Q{idx+1}: All extraction methods failed.")
            stats["failures"] += 1

    # Save Final Map
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(result_map, f, indent=4)

    # --- PRINT SUMMARY ---
    print("\n" + "="*40)
    print("📊 V2 HYBRID MINER SUMMARY")
    print("="*40)
    print(f"✅ Primary (CirraScale) Success: {stats['primary_success']}")
    print(f"⚓ Fallback (Anchor) Rescues  : {stats['anchor_rescues']}")
    print(f"❌ Total Failures            : {stats['failures']}")
    print("="*40 + "\n")

if __name__ == "__main__":
    main()
