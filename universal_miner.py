#!/usr/bin/env python3
"""
universal_miner.py

Advanced multi-modal exam ETL pipeline. Normalizes complex question types
including Hotspot (Click Region, Yes/No Matrix, Dropdown), Drag-and-Drop ordering,
and multi-question Case Scenarios into a relational LMS import schema.
"""

import argparse
import hashlib
import json
import logging
import re
import uuid
from typing import Dict, List, Optional
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Default Constants
DEFAULT_CATEGORY_NAME = "IT & Technology"
DEFAULT_COLLECTION_NAME = "General Certification"
DEFAULT_PASSMARK = 70
DEFAULT_POINTS = 1
DEFAULT_INSTRUCTOR = "Demo Instructor"


def generate_text_hash(text: str) -> str:
    """Generate collision-resistant hash ignoring case, punctuation, and whitespace."""
    if not text or pd.isna(text):
        return ""
    normalized = re.sub(r"[\W_]+", "", str(text).lower())
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def make_key(prefix: str, base: str) -> str:
    """Generates a deterministic entity key based on prefix and text hash."""
    if not base or pd.isna(base):
        return f"{prefix}_{str(uuid.uuid4())[:8].upper()}"
    clean = re.sub(r"[^A-Za-z0-9]", "", str(base))
    content_hash = hashlib.md5(str(base).encode("utf-8")).hexdigest()[:6].upper()
    short_name = clean[:10].upper()
    return f"{prefix}_{short_name}_{content_hash}"


def clean_text(text) -> str:
    if pd.isna(text):
        return ""
    return str(text).strip()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes input column headers to canonical pipeline names."""
    cols = {c.lower().replace(" ", "_").replace(".", ""): c for c in df.columns}
    mapping = {
        "question": "Question",
        "options": "Options",
        "correct_options": "Correct_Options",
        "answers": "Correct_Options",
        "explanation": "Explanation",
        "hints": "Hints",
        "scenario": "Scenario",
        "question_type": "Question_Type",
        "type": "Question_Type",
        "hotspot_variant": "Hotspot_Variant",
        "variant": "Hotspot_Variant",
        "category": "Category",
        "collection": "Collection",
        "quiz": "Quiz",
        "difficulty": "difficulty",
        "has_image": "has_image",
    }
    renamed = {}
    for standard, target in mapping.items():
        if standard in cols:
            renamed[cols[standard]] = target

    return df.rename(columns=renamed)


KEYWORD_TAG_MAP = {
    r"\b(azure\s*ad|entra)\b": "identity",
    r"\bconditional access\b": "conditional-access",
    r"\bmfa\b": "mfa",
    r"\brbac\b": "rbac",
    r"\bkey vault\b": "key-vault",
    r"\bmanaged identity\b": "managed-identity",
    r"\bpolicy\b": "policy",
    r"\bblob\b|\bstorage account\b": "storage",
    r"\bcosmos db\b": "cosmosdb",
    r"\bsql\b": "sql",
    r"\bvirtual machine\b|\bvm\b": "compute",
    r"\baks\b|\bkubernetes\b": "containers",
    r"\bvnet\b|\bnsg\b": "networking",
    r"\bmonitor\b": "monitoring",
}


def infer_tags(text_content: str, title: str) -> str:
    tags = set()
    if title:
        m = re.search(r"\b([a-z]{1,3}-\d{2,4})\b", title.lower())
        if m:
            tags.add(m.group(1).upper())
    content = text_content.lower()
    for pat, tag in KEYWORD_TAG_MAP.items():
        if re.search(pat, content):
            tags.add(tag)
    return ",".join(list(tags)[:8])


def detect_hotspot_variant(q_text: str, options_str: str) -> str:
    """Infers hotspot variant (click_region, yes_no_matrix, dropdown) when unassigned."""
    q_lower = q_text.lower()
    opt_lower = options_str.lower()

    if "[slot" in q_lower or "[slot" in opt_lower:
        return "dropdown"
    if "select yes" in q_lower or "true or false" in q_lower:
        return "yes_no_matrix"
    return "click_region"


def parse_options_v2(question_key: str, q_type: str, variant: Optional[str], options_str: str, correct_str: str) -> List[Dict]:
    """
    Parses option strings into structured option records formatted
    with metadata for interactive UI components.
    """
    options_rows = []
    options_str = clean_text(options_str)
    correct_str = clean_text(correct_str)

    if not options_str:
        return []

    # Split option choices on letter boundaries or semicolons
    if re.search(r"\b[A-Za-z]\)", options_str):
        raw_options = re.split(r";\s*(?=[A-Za-z]\))", options_str)
    else:
        raw_options = options_str.split(";")

    correct_letters = set(re.findall(r"\b([A-Za-z])\)", correct_str))
    correct_sequence = [clean_text(x) for x in correct_str.split(";")]

    for idx, opt_raw in enumerate(raw_options, 1):
        opt_text = opt_raw.strip()
        if not opt_text:
            continue

        match = re.match(r"^([A-Za-z])\)\s*(.*)", opt_text)
        if match:
            letter = match.group(1).upper()
            text_body = match.group(2).strip()
        else:
            letter = chr(64 + idx)
            text_body = opt_text

        is_correct = False
        correct_order = None
        metadata_json = None

        if q_type == "drag_drop":
            # Match item sequence position in the correct ordering array
            for seq_idx, correct_item in enumerate(correct_sequence, 1):
                if text_body.lower() in correct_item.lower():
                    correct_order = seq_idx
                    break
            is_correct = False

        elif q_type == "hotspot" and variant == "yes_no_matrix":
            # Evaluate statement against Yes/No answers
            is_correct = False
            for correct_item in correct_sequence:
                if text_body.lower() in correct_item.lower() and "yes" in correct_item.lower():
                    is_correct = True
                    break

        elif q_type == "hotspot" and variant == "dropdown":
            # Extract slot identifiers and multiple choice items
            match_slot = re.search(r"\[(SLOT\d+)\]", opt_text, re.IGNORECASE)
            slot_id = match_slot.group(1).upper() if match_slot else f"SLOT{idx}"

            raw_choices = re.sub(r"\[SLOT\d+\]", "", opt_text).strip()
            choices_list = [c.strip() for c in re.split(r"[,|]", raw_choices) if c.strip()]

            correct_choice = choices_list[0] if choices_list else text_body
            for correct_item in correct_sequence:
                if slot_id.lower() in correct_item.lower() or text_body.lower() in correct_item.lower():
                    for choice in choices_list:
                        if choice.lower() in correct_item.lower():
                            correct_choice = choice
                            break

            metadata_json = json.dumps({
                "slotId": slot_id,
                "choices": choices_list if choices_list else [text_body],
                "correctChoice": correct_choice
            })
            is_correct = True
            text_body = slot_id

        elif q_type == "hotspot" and variant == "click_region":
            # Standard click-region metadata
            metadata_json = json.dumps({
                "variant": "click_region",
                "shape": "rect",
                "coords": {"x": 10, "y": 10 + (idx * 10), "width": 50, "height": 50}
            })
            is_correct = bool(letter in correct_letters or text_body in correct_str)

        else:
            # Multiple Choice / Multiple Answer
            if letter in correct_letters:
                is_correct = True
            elif text_body in correct_str and len(text_body) > 1:
                is_correct = True

        options_rows.append({
            "QuestionKey": question_key,
            "Text": text_body,
            "IsCorrect": is_correct,
            "OrderIndex": idx,
            "Metadata": metadata_json,
            "CorrectOrder": correct_order
        })

    return options_rows


def main():
    parser = argparse.ArgumentParser(description="Multi-modal question bank transformation pipeline.")
    parser.add_argument("--input", required=True, help="Path to input Excel file")
    parser.add_argument("--output", required=True, help="Path to output Excel workbook")
    parser.add_argument("--collection", required=False, help="Collection name override")
    parser.add_argument("--lookup", required=False, help="Path to url_map.json")
    args = parser.parse_args()

    try:
        df = pd.read_excel(args.input)
        df = normalize_columns(df)
        original_count = len(df)

        # Sanitize null string artifacts
        df = df.replace(to_replace=r"(?i)^null$", value="", regex=True)

        # Deduplicate overlapping questions
        df["QuestionHash"] = df["Question"].apply(lambda x: generate_text_hash(str(x)))
        df = df.drop_duplicates(subset=["QuestionHash"], keep="first")
        dupes_dropped = original_count - len(df)
        if dupes_dropped > 0:
            logger.info("Removed %d duplicate questions.", dupes_dropped)

        # Filter rows missing option text
        df = df.dropna(subset=["Options"])
        df = df[df["Options"].astype(str).str.strip() != ""]

    except Exception as e:
        logger.error("Failed to load or clean input file: %s", e)
        return

    # Load Image Lookup if present
    image_lookup = {}
    if args.lookup:
        try:
            with open(args.lookup, "r", encoding="utf-8") as f:
                image_lookup = json.load(f)
            logger.info("Loaded %d image URL mappings.", len(image_lookup))
        except Exception as e:
            logger.warning("Could not read image lookup file: %s", e)

    tbl_questions, tbl_options, tbl_scenarios = [], [], []
    tbl_quizzes, tbl_collections, tbl_categories = {}, {}, {}
    seen_scenarios = {}
    quiz_counters = {}

    for idx, row in df.iterrows():
        cat_name = clean_text(row.get("Category")) or DEFAULT_CATEGORY_NAME
        col_name = clean_text(row.get("Collection")) or args.collection or DEFAULT_COLLECTION_NAME
        quiz_title = clean_text(row.get("Quiz")) or f"{col_name} - Batch 1"

        cat_key = make_key("CAT", cat_name)
        col_key = make_key("COL", col_name)
        quiz_key = make_key("QUIZ", quiz_title)

        if cat_key not in tbl_categories:
            tbl_categories[cat_key] = {
                "CategoryKey": cat_key,
                "Name": cat_name,
                "Description": "",
                "IsActive": True,
            }
        if col_key not in tbl_collections:
            tbl_collections[col_key] = {
                "CollectionKey": col_key,
                "Name": col_name,
                "CategoryKey": cat_key,
                "Difficulty": "medium",
                "IsPublic": True,
            }
        if quiz_key not in tbl_quizzes:
            tags = infer_tags(str(row.get("Question")), quiz_title)
            tbl_quizzes[quiz_key] = {
                "QuizKey": quiz_key,
                "Title": quiz_title,
                "CollectionKey": col_key,
                "PassMark": DEFAULT_PASSMARK,
                "IsPublic": True,
                "Tags": tags,
            }

        quiz_counters.setdefault(quiz_key, 0)
        quiz_counters[quiz_key] += 1
        q_key = f"Q-{quiz_key}-{quiz_counters[quiz_key]:03d}"
        q_type = clean_text(row.get("Question_Type", "multiple_choice")).lower()
        q_text = clean_text(row.get("Question"))

        # Determine hotspot variant
        q_variant = clean_text(row.get("Hotspot_Variant", ""))
        if q_variant.lower() in ("null", ""):
            q_variant = detect_hotspot_variant(q_text, str(row.get("Options"))) if q_type == "hotspot" else None

        # Case Study / Scenario validation & deduplication
        scenario_key = None
        scen_text = clean_text(row.get("Scenario"))
        if scen_text and scen_text.lower() != "null" and len(scen_text) > 30 and scen_text.lower() not in q_text.lower():
            scen_fingerprint = generate_text_hash(scen_text)
            if scen_fingerprint in seen_scenarios:
                scenario_key = seen_scenarios[scen_fingerprint]
            else:
                scenario_key = make_key("SCN", scen_fingerprint[:8])
                seen_scenarios[scen_fingerprint] = scenario_key
                tbl_scenarios.append({
                    "ScenarioKey": scenario_key,
                    "QuizKey": quiz_key,
                    "Title": f"Case Study / Exhibit {len(seen_scenarios)}",
                    "Context": scen_text,
                })

        # Resolve media URL
        media_val = ""
        if q_text in image_lookup:
            media_val = image_lookup[q_text]
        elif str(row.get("has_image")).lower() in ("true", "1", "yes"):
            media_val = "1"

        tbl_questions.append({
            "QuestionKey": q_key,
            "QuizKey": quiz_key,
            "Type": q_type,
            "Text": q_text,
            "Explanation": clean_text(row.get("Explanation")),
            "Points": DEFAULT_POINTS,
            "Order": quiz_counters[quiz_key],
            "ScenarioKey": scenario_key,
            "ScenarioOrder": 1 if scenario_key else None,
            "Hints": clean_text(row.get("Hints")),
            "PartialScoring": bool(q_type in ("multiple_answer", "drag_drop")),
            "Variant": q_variant,
            "MediaUrl": media_val,
        })

        opt_rows = parse_options_v2(q_key, q_type, q_variant, row.get("Options"), row.get("Correct_Options"))
        tbl_options.extend(opt_rows)

    with pd.ExcelWriter(args.output, engine="openpyxl") as writer:
        pd.DataFrame(list(tbl_categories.values())).to_excel(writer, "Categories", index=False)
        pd.DataFrame(list(tbl_collections.values())).to_excel(writer, "Collections", index=False)
        pd.DataFrame(list(tbl_quizzes.values())).to_excel(writer, "Quizzes", index=False)
        pd.DataFrame(tbl_scenarios).to_excel(writer, "Scenarios", index=False)
        pd.DataFrame(tbl_questions).to_excel(writer, "Questions", index=False)
        pd.DataFrame(tbl_options).to_excel(writer, "Options", index=False)

    logger.info("Transformation complete: %d questions and %d options processed.",
                len(tbl_questions), len(tbl_options))
    logger.info("Output written to: %s", args.output)


if __name__ == "__main__":
    main()
