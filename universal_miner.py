import sys
import json
import requests
import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import io

# --- CONFIGURATION ---
# Tesseract config for sparse text (like exam diagrams)
OCR_CONFIG = r'--oem 3 --psm 6'

def download_pdf(url, save_path):
    print(f"Downloading PDF from {url}...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)
        print("Download complete.")
        return True
    else:
        print(f"Failed to download: {response.status_code}")
        return False

def run_ocr(image_bytes):
    """Converts image bytes to text using Tesseract."""
    try:
        image = Image.open(io.BytesIO(image_bytes))
        # Skip tiny icons/lines
        if image.width < 50 or image.height < 20: return ""
        
        text = pytesseract.image_to_string(image, config=OCR_CONFIG)
        return text.strip()
    except Exception as e:
        return ""

def process_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    full_content = []

    print(f"Processing {len(doc)} pages...")

    for page_num, page in enumerate(doc):
        page_items = []

        # 1. EXTRACT TEXT
        text_blocks = page.get_text("blocks")
        for b in text_blocks:
            # b = (x0, y0, x1, y1, text, ...)
            text = b[4].strip()
            if text:
                page_items.append({
                    "type": "text",
                    "content": text,
                    "y": b[1],
                    "x": b[0]
                })

        # 2. EXTRACT IMAGES & RUN OCR
        image_list = page.get_images(full=True)
        for img_index, img in enumerate(image_list):
            xref = img[0]
            
            # Get location to sort correctly
            rects = page.get_image_rects(xref)
            if not rects: continue
            rect = rects[0]
            
            base_image = doc.extract_image(xref)
            ocr_text = run_ocr(base_image["image"])
            
            if ocr_text:
                # Mark it as visual content
                formatted = f"\n[VISUAL START]\n{ocr_text}\n[VISUAL END]\n"
                page_items.append({
                    "type": "ocr_image",
                    "content": formatted_text,
                    "y": rect.y0,
                    "x": rect.x0
                })

        # 3. SORT BY READING ORDER (Top -> Bottom)
        page_items.sort(key=lambda k: (k['y'], k['x']))

        # 4. MERGE INTO SINGLE STRING
        page_text = "\n\n".join([item['content'] for item in page_items])
        
        full_content.append({
            "page": page_num + 1,
            "text": page_text
        })

    return full_content

def main():
    # Arguments from GitHub Actions
    if len(sys.argv) < 3:
        print("Usage: python universal_miner.py <pdf_url> <callback_url>")
        sys.exit(1)

    pdf_url = sys.argv[1]
    callback_url = sys.argv[2]
    temp_pdf = "temp_exam.pdf"

    # 1. Download
    if not download_pdf(pdf_url, temp_pdf):
        sys.exit(1)

    # 2. Process
    try:
        extracted_data = process_pdf(temp_pdf)
        
        # 3. Send back to n8n
        print(f"Sending {len(extracted_data)} pages to n8n...")
        payload = {"data": extracted_data, "status": "success"}
        
        # We use a large timeout because n8n might take a second to accept it
        r = requests.post(callback_url, json=payload, timeout=30)
        print(f"n8n Response: {r.status_code}")

    except Exception as e:
        print(f"Error: {e}")
        # Send error to n8n so workflow doesn't hang
        requests.post(callback_url, json={"status": "error", "message": str(e)})
        sys.exit(1)

if __name__ == "__main__":
    main()
