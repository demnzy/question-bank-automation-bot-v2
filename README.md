# Question Bank Automation Bot (v2)

Extends [v1](https://github.com/demnzy/question-bank-automation-bot) to the question types that depend on images or structure: **hotspot** (three variants), **drag-and-drop ordering** and **case-study scenarios**, alongside standard multiple choice and multiple answer. It turns the LLM-extracted table for an exam dump into an LMS-ready workbook, and for questions that reference a figure it crops the figure out of the source PDF, uploads it, and stores the URL in the question's `MediaUrl` column.

**Stack:** Python 3.9, pandas, openpyxl, PyMuPDF, Tesseract (pytesseract), GitHub Actions, n8n

## Pipeline

```
n8n workflow A
  LLM extraction of the PDF -> Excel table + per-figure bounding boxes
  sends a repository_dispatch event
        |
        v
GitHub Actions (.github/workflows/transform_v2.yml)
  1. download the Excel (Google Sheets links are rewritten to .xlsx export links)
  2. download the PDF if a pdf_url was given
  3. write image_lookup_json from the payload to coords.json
  4. universal_image_miner.py  -> url_map.json   {question text: image URL}
  5. universal_miner.py        -> <exam>_Final.xlsx
  6. POST the workbook to the n8n callback URL
        |
        v
n8n workflow B
  receives the file together with folder_id and user_email
```

If no `pdf_url` is supplied, the miner step is skipped and an empty `url_map.json` is used, so text-only exams still go through.

## Supported question types

| `Question_Type` | Handling |
|---|---|
| `multiple_choice`, `multiple_answer` | `A) text; B) text` options, letters in `Correct_Options` set `IsCorrect`. `PartialScoring` is on for multiple answer |
| `hotspot` | Variant stored in `Variant`: `click_region`, `yes_no_matrix` or `dropdown` (see below) |
| `drag_drop` | Each option gets a `CorrectOrder` from its position in the answer sequence. `PartialScoring` is on |

Hotspot variants come from a `Hotspot_Variant` column when the LLM supplies one. Otherwise they are inferred: `[SLOT` in the question or options means `dropdown`, "select yes" or "true or false" means `yes_no_matrix`, anything else is `click_region`.

| Variant | Option rows |
|---|---|
| `yes_no_matrix` | One row per statement, `IsCorrect` is true when the answer for that statement is Yes |
| `dropdown` | One row per slot. `Text` holds the slot id and `Metadata` holds `{"slotId", "choices", "correctChoice"}` as JSON |
| `click_region` | `Metadata` holds a rectangle (`variant`, `shape`, `coords`). **These coordinates are placeholders, not detected regions** (see limitations) |

**Scenarios.** Where a row has a `Scenario` (case-study or exhibit text longer than 30 characters that is not already in the question), it goes to a `Scenarios` sheet, deduplicated by a normalised-text fingerprint so questions sharing a case study share one scenario row.

## Input

Excel table (first sheet). Column names are matched case-insensitively, with spaces and dots normalised.

| Column | Notes |
|---|---|
| `Question` | May include `<<IMAGE_REF_n>>` tags marking figures |
| `Options` | `A) text; B) text`. Rows with empty options are dropped |
| `Correct_Options` | Also accepted as `Correct Options` or `Answers` |
| `Question_Type`, `Hotspot_Variant` | See above |
| `Scenario`, `Explanation`, `Hints` | Optional |
| `Category`, `Collection`, `Quiz`, `difficulty` | `Collection` falls back to `--collection`, then a default |
| `has_image` | `true`/`1`/`yes` for figure questions without tags |

`Correct_Options` formats: letters (`A) text`) for choice questions, and for drag-and-drop or matrices an ordered `;`-separated list (`step one; step two`, or `Statement - Yes; Statement - No`).

## Output

`<exam>_Final.xlsx` with sheets `Categories`, `Collections`, `Quizzes`, `Scenarios`, `Questions`, `Options`.

- **Questions:** `QuestionKey`, `QuizKey`, `Type`, `Text`, `Explanation`, `Points`, `Order`, `ScenarioKey`, `ScenarioOrder`, `Hints`, `PartialScoring`, `Variant`, `MediaUrl`
- **Options:** `QuestionKey`, `Text`, `IsCorrect`, `OrderIndex`, `Metadata`, `CorrectOrder`
- Keys are deterministic (name prefix plus content hash), so re-running on the same input gives the same keys. Fixed values: `Points = 1`, `PassMark = 70`.
- `MediaUrl` is the uploaded image URL. If a question is flagged `has_image` but no URL was found, it is set to `1` as a marker for manual follow-up.

## Sanitisation (same as v1)

Before any parsing, `universal_miner.py` removes literal `null` strings, drops duplicate questions caused by overlapping extraction chunks (normalised-text MD5, first occurrence kept), and drops rows with no options.

## Image extraction

`universal_image_miner.py` tries three strategies per question and uploads the first crop it gets (200 dpi JPEG):

1. **Bounding box.** For each `<<IMAGE_REF_n>>` tag, look up `n` in `coords.json`. Each entry has a `page` (1-indexed) and `coordinates` as `"ymin,xmin,ymax,xmax"` in percent of the page size. This is the precise path; the boxes come from the extraction model in workflow A.
2. **Text anchor (fallback).** If the box is missing or invalid, search the PDF for the first 60 characters of the question (then 30) and crop 400 pt of full page width below the match.
3. **No tags.** If there is no tag but `has_image` is true, go straight to the text anchor.

The result goes into `url_map.json`, keyed by the question text, which the transformer maps into `MediaUrl`. The run prints how many images came from bounding boxes, how many were rescued by text anchoring, and how many failed.

For drag-and-drop questions that have an image but no options, the miner also runs Tesseract on the crop and writes the text to `url_map.json` under `"<question text>_OCR"`. The transformer does not currently read this entry.

## Running it

### Through GitHub Actions

Trigger with a `repository_dispatch` event of type `start-processing`:

```bash
curl -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $GITHUB_TOKEN" \
  https://api.github.com/repos/demnzy/question-bank-automation-bot-v2/dispatches \
  -d '{
    "event_type": "start-processing",
    "client_payload": {
      "collection_name": "AZ-305",
      "excel_url": "https://example.com/extracted.xlsx",
      "pdf_url": "https://example.com/exam.pdf",
      "image_lookup_json": "{\"IMAGE_REF_1\": {\"page\": 3, \"coordinates\": \"20.5,10,55,90\"}}",
      "callback_url": "https://your-n8n-host/webhook/final",
      "folder_id": "FOLDER_ID",
      "user_email": "user@example.com"
    }
  }'
```

| Payload key | Used for |
|---|---|
| `collection_name` | Sanitised into file names and passed as `--collection` |
| `excel_url` | Input table (Google Sheets `/edit` links are converted to `/export?format=xlsx`) |
| `pdf_url` | Optional. Without it, image extraction is skipped |
| `image_lookup_json` | Optional. Bounding-box map written to `coords.json` |
| `callback_url` | Where the workbook is POSTed |
| `folder_id`, `user_email` | Passed through to n8n workflow B unchanged |

**Repository secrets:**

| Secret | Description |
|---|---|
| `SUCCEED_EMAIL` | LMS account used to upload images (the workflow fails early if it is missing) |
| `SUCCEED_PASSWORD` | Password for that account |

The scripts read these from environment variables. Never commit credentials to the repository.

### Locally

```bash
pip install -r requirements.txt            # also needs the tesseract binary
export SUCCEED_EMAIL="..." SUCCEED_PASSWORD="..."

python universal_image_miner.py exam.xlsx exam.pdf coords.json url_map.json
python universal_miner.py \
  --input exam.xlsx \
  --output exam_Final.xlsx \
  --lookup url_map.json \
  --collection "AZ-305"
```

## Differences from v1

| | v1 | v2 |
|---|---|---|
| Question types | multiple choice, multiple answer, true/false | adds hotspot (3 variants), drag-and-drop, scenarios |
| Output template | Column names read from `SQ_template.xlsx` | Columns fixed in code, plus a `Scenarios` sheet |
| Image cropping | Text anchor only | Bounding box first, text anchor fallback |
| Input | Excel, CSV or JSON | Excel |
| Optional PDF | No | Yes |

## Known limitations

- Click-region hotspot rectangles are generated placeholders. The real clickable regions still need to be set in the LMS.
- OCR text for drag-and-drop images is written to `url_map.json` but is not yet used by the transformer.
- Images are matched to questions by exact question text, so edits to the text between extraction and transformation break the match (the question then gets the `1` placeholder).
- The tag keyword map is Azure-specific, and the defaults are generic (`IT & Technology`, `General Certification`).
- Answer parsing for matrices and dropdowns relies on substring matching against the answer text, so unusual answer wording can be mis-assigned. Spot-check these types.

## Repository layout

```
.github/workflows/transform_v2.yml   Actions workflow (repository_dispatch)
universal_image_miner.py             crop, OCR-rescue and upload question images
universal_miner.py                   transform the table into the LMS workbook
requirements.txt
```

Built by [Oluwatobiloba (Daniel) Davies](https://github.com/demnzy).
