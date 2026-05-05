# QB Automation Bot — v2

> Extended question bank automation pipeline supporting **seven question types** — from standard MCQ and True/False through to Fill in the Gaps, Hotspot Simulations, Matching, Ordering, and Short Answer — all transformed from raw exam PDFs to precise LMS-ready schemas.

**Stack:** Python · PyMuPDF (fitz) · GitHub Actions · n8n

> v2 is a superset of [QB Bot v1](https://github.com/demnzy/qb-bot-v1). Everything v1 does, v2 does — plus significantly more.

---

## Table of Contents

- [Overview](#overview)
- [How It Works](#how-it-works)
- [Question Types Supported](#question-types-supported)
- [Repository Structure](#repository-structure)
- [Setup](#setup)
  - [Secrets & Environment Variables](#secrets--environment-variables)
  - [Triggering the Workflow](#triggering-the-workflow)
- [Output Schema](#output-schema)
  - [Fill in the Gaps](#fill-in-the-gaps)
  - [Hotspot Simulation](#hotspot-simulation)
  - [Matching](#matching)
  - [Ordering](#ordering)
  - [Short Answer](#short-answer)
- [Local Development](#local-development)
- [Differences from v1](#differences-from-v1)

---

## Overview

QB Bot v2 extends the v1 pipeline to handle the full complexity of modern LMS question formats. Where v1 handles flat, text-only question types, v2 introduces a modular classifier architecture that detects structural patterns in PDFs corresponding to interactive and compound question types — including cloze-style fill-in-the-gaps, image-referenced hotspot questions, and multi-column matching exercises.

The automation chain is identical to v1: n8n triggers a GitHub Actions workflow, Python runs the extraction and classification inside the Action, and structured JSON output is pushed to the configured storage target for downstream LMS ingestion.

---

## How It Works

```
n8n Workflow
    │
    │  Webhook trigger (POST with PDF reference or payload)
    ▼
GitHub Actions (repository_dispatch)
    │
    ├── Checkout repo
    ├── Set up Python environment
    ├── Install dependencies
    └── Run transform.py
            │
            ├── Load PDF with PyMuPDF (fitz)
            ├── Extract raw text and layout metadata page by page
            ├── Run ClassifierPipeline
            │       │
            │       ├── MCQClassifier
            │       ├── TrueFalseClassifier
            │       ├── FillInGapsClassifier       ← new in v2
            │       ├── HotspotClassifier          ← new in v2
            │       ├── MatchingClassifier         ← new in v2
            │       ├── OrderingClassifier         ← new in v2
            │       └── ShortAnswerClassifier      ← new in v2
            │
            ├── Merge and deduplicate detected questions
            ├── Validate against output schema v2
            └── Write structured JSON per question type
                    │
                    ▼
            Push to repo / storage bucket
                    │
                    ▼
            n8n routes output to LMS API
```

The classifier pipeline runs each detector in sequence. Questions that match multiple classifiers (edge cases in badly formatted PDFs) are resolved by a confidence-scoring tiebreaker that selects the most structurally complete interpretation.

---

## Question Types Supported

| Type | v1 | v2 | Detection Approach |
|---|:---:|:---:|---|
| Multiple Choice (MCQ) | ✅ | ✅ | Option labels A–D in proximity to stem |
| True / False | ✅ | ✅ | Boolean keywords after statement |
| Fill in the Gaps | — | ✅ | Blank markers (`___`, `[...]`, numbered gaps) in sentence body |
| Hotspot Simulation | — | ✅ | Image reference with labelled target zones or coordinate markers |
| Matching | — | ✅ | Two-column layout with paired items and connectors |
| Ordering / Sequencing | — | ✅ | Numbered or lettered items presented out of sequence with ordering instruction |
| Short Answer | — | ✅ | Open-ended stem with no options and a defined answer field |

---

## Repository Structure

```
qb-bot-v2/
│
├── .github/
│   └── workflows/
│       └── transform.yml            # GitHub Actions workflow definition
│
├── scripts/
│   ├── transform.py                 # Entry point — orchestrates the full pipeline
│   ├── classifier_pipeline.py       # Runs all classifiers in sequence
│   │
│   └── classifiers/
│       ├── base.py                  # BaseClassifier abstract class
│       ├── mcq.py
│       ├── true_false.py
│       ├── fill_in_gaps.py
│       ├── hotspot.py
│       ├── matching.py
│       ├── ordering.py
│       └── short_answer.py
│
├── schemas/
│   ├── question_schema_v1.json      # v1 schema (MCQ + T/F) — kept for reference
│   └── question_schema_v2.json      # v2 schema — all types
│
├── input/
│   └── .gitkeep
│
├── output/
│   └── .gitkeep
│
├── tests/
│   ├── test_mcq.py
│   ├── test_fill_in_gaps.py
│   ├── test_hotspot.py
│   └── test_matching.py
│
├── requirements.txt
├── .env.example
└── README.md
```

---

## Setup

### Secrets & Environment Variables

Add the following secrets under **Settings → Secrets and variables → Actions**:

| Secret | Description |
|---|---|
| `N8N_WEBHOOK_URL` | The n8n webhook URL to notify on completion |
| `STORAGE_BUCKET_URL` | Target storage URL for output JSON |
| `STORAGE_ACCESS_KEY` | Access credentials for the storage target |

For local development, copy `.env.example` to `.env`:

```env
N8N_WEBHOOK_URL=https://your-n8n-instance/webhook/qb-bot-v2
STORAGE_BUCKET_URL=https://your-bucket-url
STORAGE_ACCESS_KEY=your-access-key
```

### Triggering the Workflow

**From n8n** — use the `HTTP Request` node to fire a `repository_dispatch` event:

```
POST https://api.github.com/repos/{owner}/qb-bot-v2/dispatches

Headers:
  Authorization: Bearer <GITHUB_PAT>
  Accept: application/vnd.github.v3+json

Body:
{
  "event_type": "transform-pdf",
  "client_payload": {
    "pdf_url": "https://link-to-your-pdf.pdf",
    "exam_id": "exam_042",
    "question_types": ["mcq", "fill_in_gaps", "hotspot"]
  }
}
```

The optional `question_types` array scopes the classifier pipeline to only run the detectors you need, which speeds up processing on large PDFs where you know the content ahead of time. Omit it to run all classifiers.

**Manually (GitHub UI):**

Go to **Actions → Transform PDF v2 → Run workflow**.

**From the CLI:**

```bash
gh workflow run transform.yml \
  -f pdf_url="https://link-to-your-pdf.pdf" \
  -f exam_id="exam_042"
```

---

## Output Schema

All output is written as a JSON array. Each item contains a `type` field that identifies its structure. Examples for the question types introduced in v2:

### Fill in the Gaps

```json
{
  "type": "fill_in_gaps",
  "exam_id": "exam_042",
  "question_number": 5,
  "text": "The process by which plants convert sunlight into energy is called ___1___, and it takes place in the ___2___.",
  "gaps": {
    "1": "photosynthesis",
    "2": "chloroplast"
  },
  "explanation": ""
}
```

### Hotspot Simulation

```json
{
  "type": "hotspot",
  "exam_id": "exam_042",
  "question_number": 8,
  "instruction": "Click on the part of the cell responsible for energy production.",
  "image_reference": "figure_3_cell_diagram",
  "correct_zone": "mitochondria",
  "zones": ["nucleus", "mitochondria", "cell_membrane", "vacuole"],
  "explanation": ""
}
```

### Matching

```json
{
  "type": "matching",
  "exam_id": "exam_042",
  "question_number": 11,
  "instruction": "Match each term to its correct definition.",
  "pairs": [
    { "left": "Osmosis", "right": "Movement of water across a semi-permeable membrane" },
    { "left": "Diffusion", "right": "Net movement of particles from high to low concentration" },
    { "left": "Active transport", "right": "Movement of substances against a concentration gradient" }
  ],
  "explanation": ""
}
```

### Ordering

```json
{
  "type": "ordering",
  "exam_id": "exam_042",
  "question_number": 14,
  "instruction": "Arrange the following steps of mitosis in the correct order.",
  "items": ["Anaphase", "Telophase", "Metaphase", "Prophase"],
  "correct_order": ["Prophase", "Metaphase", "Anaphase", "Telophase"],
  "explanation": ""
}
```

### Short Answer

```json
{
  "type": "short_answer",
  "exam_id": "exam_042",
  "question_number": 17,
  "stem": "Explain the role of insulin in blood glucose regulation.",
  "sample_answer": "Insulin is released by the pancreas in response to high blood glucose levels. It stimulates cells to absorb glucose, lowering blood sugar back to a normal range.",
  "marks": 3,
  "explanation": ""
}
```

---

## Local Development

```bash
# 1. Clone the repo
git clone https://github.com/demnzy/qb-bot-v2.git
cd qb-bot-v2

# 2. Set up a virtual environment
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Drop a PDF into input/
cp your-exam.pdf input/

# 5. Run the full pipeline locally
python scripts/transform.py \
  --input input/your-exam.pdf \
  --exam_id exam_042

# Run with specific classifiers only
python scripts/transform.py \
  --input input/your-exam.pdf \
  --exam_id exam_042 \
  --types mcq fill_in_gaps hotspot

# Output: output/exam_042_transformed.json
```

**Running tests:**

```bash
pytest tests/ -v
```

---

## Differences from v1

| Feature | v1 | v2 |
|---|---|---|
| MCQ support | ✅ | ✅ |
| True/False support | ✅ | ✅ |
| Fill in the Gaps | — | ✅ |
| Hotspot Simulation | — | ✅ |
| Matching | — | ✅ |
| Ordering / Sequencing | — | ✅ |
| Short Answer | — | ✅ |
| Modular classifier architecture | — | ✅ |
| Scoped classifier runs via `question_types` | — | ✅ |
| Confidence-scoring tiebreaker | — | ✅ |
| Test suite | — | ✅ |
| Schema version | v1 | v2 |

If your PDFs only contain MCQ and True/False questions, [v1](https://github.com/demnzy/qb-bot-v1) is simpler and faster. Use v2 when your exam content includes any of the extended types.

---

Built by [Oluwatobiloba (Daniel) Davies](https://github.com/demnzy) · [LinkedIn](https://linkedin.com/in/oluwatobiloba-davies)
