# Question Bank Automation: Advanced Multi-Modal Ingestion Pipeline (v2)

An enterprise data extraction and transformation engine that extends [v1](https://github.com/demnzy/question-bank-automation-bot) to support complex interactive question formats: **Hotspot** (Click Regions, Yes/No Matrices, Dropdown Slots), **Drag-and-Drop sequence ordering**, and **Case Study / Exhibit Scenarios**, alongside standard choice types.

**Tech Stack:** Python 3.9, pandas, openpyxl, PyMuPDF (`fitz`), pytesseract, Pillow, cloudscraper, GitHub Actions, n8n.

---

## What v2 Introduces

While v1 handles standard text questions, professional certification dumps (Azure, AWS, CompTIA, Cisco) feature complex UI interaction patterns. v2 introduces:

1. **Multi-Modal Hotspot Parsing:** Automatically routes and formats three distinct UI variants:
   - `click_region`: Interactive bounding box coordinates stored in option metadata.
   - `yes_no_matrix`: Multi-statement evaluations with boolean `IsCorrect` flags.
   - `dropdown`: Dynamic slot parsing (`[SLOT1]`, `[SLOT2]`) with choices and correct values serialized into JSON metadata.
2. **Drag-and-Drop Sequence Ordering:** Parses step sequences into deterministic `CorrectOrder` ranks with `PartialScoring` enabled.
3. **Relational Case Study Scenarios:** Extracts multi-paragraph exhibits and scenarios into a dedicated `Scenarios` sheet, deduplicated via content hashing so questions sharing an exhibit link to the same parent entity.
4. **Hybrid Diagram Miner with OCR Fallback:** Uses precision bounding-box coordinates when available, falls back to geometric text-anchoring, and uses Tesseract OCR to recover unparsed labels directly from diagram images.

---

## Architecture & Data Flow

```
                      +-----------------------------+
                      |   n8n Extraction Webhook    |
                      |  (PDF -> Raw Table + BBoxes)|
                      +--------------+--------------+
                                     | repository_dispatch
                                     v
                 +---------------------------------------+
                 |    GitHub Actions CI/CD Pipeline      |
                 |     (.github/workflows/transform_v2)  |
                 +-------------------+-------------------+
                                     |
           +-------------------------+-------------------------+
           |                                                   |
           v                                                   v
+-----------------------------+                     +-----------------------+
|  universal_image_miner.py   |                     |  universal_miner.py   |
| - Bounding-box cropping     |                     | - Schema normalization|
| - Text-anchor fallback      |                     | - Hotspot & Drag/Drop |
| - Tesseract OCR label rescue|                     | - Case study scenarios|
| - Cloud media upload        |                     | - Out: 6-sheet .xlsx  |
+--------------+--------------+                     +-----------+-----------+
               |                                                ^
               +====================[ url_map.json ]============+
                                                                |
                                                                v
                                                    +-----------------------+
                                                    |  n8n Callback Webhook |
                                                    | (Delivers Final .xlsx)|
                                                    +-----------------------+
```

---

## Question Types & Metadata Specifications

### 1. Hotspot Formats
- **Yes/No Matrix (`yes_no_matrix`):** Evaluates multi-part statements. Each row corresponds to a statement, and `IsCorrect` indicates whether "Yes" is the valid choice.
- **Dropdown Slots (`dropdown`):** Questions with inline dropdowns (`[SLOT1]`, `[SLOT2]`). The option text records the slot ID, while `Metadata` stores:
  ```json
  {
    "slotId": "SLOT1",
    "choices": ["East US", "West Europe", "Central US"],
    "correctChoice": "East US"
  }
  ```
- **Click Region (`click_region`):** Generates coordinate bounding box metadata for image regions to support spatial click validation.

### 2. Drag & Drop Ordering (`drag_drop`)
Parses execution sequences (e.g. deployment orders, command pipelines) and assigns sequential ranks (`CorrectOrder: 1, 2, 3...`) with `PartialScoring = True`.

### 3. Case Study Exhibits (`Scenarios`)
When questions reference multi-part scenarios (>30 characters of context), the pipeline creates a normalized entry in the `Scenarios` sheet (`SCN-<HASH>`) and references it from `Questions.ScenarioKey`.

---

## Relational Output Structure

The output workbook (`<Exam>_Final.xlsx`) provides 6 relational sheets:

| Sheet | Primary Key | Foreign Keys | Key Attributes |
|---|---|---|---|
| **Categories** | `CategoryKey` | — | `Name`, `Description`, `IsActive` |
| **Collections** | `CollectionKey` | `CategoryKey` | `Name`, `Difficulty`, `IsPublic` |
| **Quizzes** | `QuizKey` | `CollectionKey` | `Title`, `PassMark`, `Tags`, `IsPublic` |
| **Scenarios** | `ScenarioKey` | `QuizKey` | `Title`, `Context` (Case study text) |
| **Questions** | `QuestionKey` | `QuizKey`, `ScenarioKey` | `Text`, `Type`, `Variant`, `MediaUrl`, `PartialScoring` |
| **Options** | `OptionKey` | `QuestionKey` | `Text`, `IsCorrect`, `CorrectOrder`, `Metadata` |

---

## Local Usage

### Installation

```bash
git clone https://github.com/demnzy/question-bank-automation-bot-v2.git
cd question-bank-automation-bot-v2
pip install -r requirements.txt
```

### Environment Configuration

```bash
export SUCCEED_EMAIL="your-api-email"
export SUCCEED_PASSWORD="your-api-password"
```

### Execution

```bash
# 1. Extract figures via bounding-box or anchor fallback
python universal_image_miner.py sample.xlsx sample.pdf coords.json url_map.json

# 2. Transform into full 6-sheet LMS workbook
python universal_miner.py \
  --input sample.xlsx \
  --output Exam_Final.xlsx \
  --lookup url_map.json \
  --collection "AZ-104 Certification"
```

---

## CI/CD Dispatch Specification

Trigger the workflow (`.github/workflows/transform_v2.yml`) via GitHub REST API:

```bash
curl -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $GITHUB_TOKEN" \
  https://api.github.com/repos/demnzy/question-bank-automation-bot-v2/dispatches \
  -d '{
    "event_type": "start-processing",
    "client_payload": {
      "collection_name": "AZ-104",
      "excel_url": "https://docs.google.com/spreadsheets/d/EXAMPLE/edit",
      "pdf_url": "https://example.com/exam.pdf",
      "image_lookup_json": "{\"IMAGE_REF_1\": {\"page\": 3, \"coordinates\": \"10,20,40,80\"}}",
      "callback_url": "https://n8n.example.com/webhook/complete",
      "folder_id": "TARGET_FOLDER_ID",
      "user_email": "admin@example.com"
    }
  }'
```
