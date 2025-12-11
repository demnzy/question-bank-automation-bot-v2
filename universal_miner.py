import sys
import json
import requests
import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import io
import os

# --- CONFIGURATION ---
# --psm 6 is "Sparse Text" mode, ideal for diagrams/tables
OCR_CONFIG = r'--oem 3 --psm 6'

def download_file(url, local_filename):
    print(f"Downloading PDF from {url}...")
    try:
        # Stream download to handle large files effectively
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("Download complete.")
        return True
    except Exception as e:
        print(f"Download failed: {e}")
        return False

def run_ocr(image_bytes):
    """
    Converts image bytes to text using Tesseract.
    Filters out tiny noise icons to save CPU time.
    """
    try:
        image = Image.open(io.BytesIO(image_bytes))
        
        # Skip tiny images (e.g., bullet points, small icons)
        if image.width < 50 or image.height < 20: 
            return ""
        
        text = pytesseract.image_to_string(image, config=OCR_CONFIG)
        return text.strip()
    except Exception:
        return ""

def process_pdf(path):
    """
    Iterates through the PDF page by page.
    Mixes standard text blocks with OCR'd image blocks based on Y-position.
    """
    doc = fitz.open(path)
    pages_data = []

    print(f"Processing {len(doc)} pages...")

    for page_num, page in enumerate(doc):
        # We collect ALL items (Text and Images) into a list with their Y-position
        # so we can sort them by natural reading order (Top -> Bottom).
        page_items = []

        # 1. GET STANDARD TEXT
        # block format: (x0, y0, x1, y1, "text", block_no, block_type)
        text_blocks = page.get_text("blocks")
        for b in text_blocks:
            text_content = b[4].strip()
            if text_content:
                page_items.append({
                    "content": text_content,
                    "y": b[1], # Top Y coordinate
                    "x": b[0], # Left X coordinate
                    "type": "text"
                })

        # 2. GET IMAGES -> RUN OCR -> WRAP IN TAGS
        image_list = page.get_images(full=True)
        for img_info in image_list:
            xref = img_info[0]
            
            # Get location on page to sort correctly
            rects = page.get_image_rects(xref)
            if not rects: continue
            
            # Use the first instance of the image
            rect = rects[0]
            
            # Extract and OCR
            base_image = doc.extract_image(xref)
            ocr_text = run_ocr(base_image["image"])
            
            if ocr_text:
                # MARKER: This is the standardized signal for your AI Agent
                formatted_block = f"\n[VISUAL START]\n{ocr_text}\n[VISUAL END]\n"
                
                page_items.append({
                    "content": formatted_block,
                    "y": rect.y0,
                    "x": rect.x0,
                    "type": "image"
                })

        # 3. SORT BY READING ORDER
        # Primary sort: Vertical (Y), Secondary sort: Horizontal (X)
        page_items.sort(key=lambda k: (k['y'], k['x']))

        # 4. MERGE INTO SINGLE STRING FOR THIS PAGE
        # This creates the "Digital Twin" text stream
        page_blob = "\n\n".join([item['content'] for item in page_items])
        
        pages_data.append({
            "page": page_num + 1,
            "text": page_blob
        })

    return pages_data

def main():
    # CLI Arguments passed by GitHub Actions YAML
    if len(sys.argv) < 3:
        print("Usage: python universal_miner.py <pdf_url> <callback_url>")
        sys.exit(1)

    pdf_url = sys.argv[1]
    callback_url = sys.argv[2]
    temp_file = "exam_dump_temp.pdf"

    # --- STEP 1: DOWNLOAD ---
    if not download_file(pdf_url, temp_file):
        # Notify n8n of failure so it doesn't wait forever
        error_payload = {"status": "error", "message": "Failed to download PDF from URL provided."}
        requests.post(callback_url, json=error_payload)
        sys.exit(1)

    # --- STEP 2: PROCESS (EXTRACT + OCR) ---
    try:
        extracted_pages = process_pdf(temp_file)
        
        # --- STEP 3: SEND BACK TO n8n ---
        print(f"Extraction complete. Sending {len(extracted_pages)} pages to n8n...")
        
        payload = {
            "status": "success",
            "data": extracted_pages  # Array of { page: 1, text: "..." }
        }
        
        # Post back to the Wait Node Webhook
        r = requests.post(callback_url, json=payload, timeout=30)
        
        if r.status_code == 200:
            print("Successfully sent data to n8n.")
        else:
            print(f"Warning: n8n returned status code {r.status_code}")

    except Exception as e:
        print(f"Critical Error: {e}")
        # Send error to n8n
        error_payload = {"status": "error", "message": str(e)}
        requests.post(callback_url, json=error_payload)
        sys.exit(1)

if __name__ == "__main__":
    main()
