#!/usr/bin/env python3
"""
universal_miner.py (V2.1 - Hotspot Metadata Support)

Updates:
- Questions Table: Added 'Variant' column.
- Options Table: Replaced 'HotspotCoords' with 'Metadata' (JSON).
- Logic: Infer Hotspot Variants (yes_no_matrix, click_region, dropdown).
"""

import argparse
import re
import json
import pandas as pd
import uuid
import hashlib
from pathlib import Path
from typing import Dict, List, Any

# --- CONFIGURATION ---
DEFAULT_CATEGORY_NAME = "IT & Technology"
DEFAULT_COLLECTION_NAME = "General Certification"
DEFAULT_PASSMARK = 70
DEFAULT_POINTS = 1
DEFAULT_INSTRUCTOR = "Demo Instructor"

# --------------------- HELPERS ---------------------

def make_key(prefix: str, base: str) -> str:
    """Generates a deterministic key based on content hash."""
    if not base or pd.isna(base): 
        return f"{prefix}_{str(uuid.uuid4())[:8].upper()}"
    clean = re.sub(r"[^A-Za-z0-9]", "", str(base))
    content_hash = hashlib.md5(str(base).encode()).hexdigest()[:6].upper()
    short_name = clean[:10].upper()
    return f"{prefix}_{short_name}_{content_hash}"

def clean_text(text):
    if pd.isna(text): return ""
    return str(text).strip()

def clean_hint_text(h: str) -> str:
    if pd.isna(h) or h is None: return ""
    s = str(h).strip()
    s = re.sub(r'^\s*hint\s*:\s*', '', s, flags=re.IGNORECASE)
    return s

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes input column names."""
    cols = {c.lower().replace(" ", "_").replace(".", ""): c for c in df.columns}
    mapping = {
        "question": "Question", "options": "Options", 
        "correct_options": "Correct_Options", "answers": "Correct_Options",
        "explanation": "Explanation", "hints": "Hints", "scenario": "Scenario",
        "question_type": "Question_Type", "type": "Question_Type",
        "category": "Category", "collection": "Collection", "quiz": "Quiz",
        "difficulty": "difficulty", "has_image": "has_image",
        "tag": "Tag", "ispublic": "isPublic"
    }
    renamed = {}
    for standard, target in mapping.items():
        if standard in cols: renamed[cols[standard]] = target
    
    df = df.rename(columns=renamed)
    return df

# --------------------- TAGGING (V1 LOGIC) ---------------------
KEYWORD_TAG_MAP = {
    r"\b(azure\s*ad|entra)\b": "identity", r"\bconditional access\b": "conditional-access",
    r"\bmfa\b": "mfa", r"\brbac\b": "rbac", r"\bkey vault\b": "key-vault",
    r"\bmanaged identity\b": "managed-identity", r"\bpolicy\b": "policy", 
    r"\bblob\b|\bstorage account\b": "storage", r"\bcosmos db\b": "cosmosdb", r"\bsql\b": "sql",
    r"\bvirtual machine\b|\bvm\b": "compute", r"\baks\b|\bkubernetes\b": "containers",
    r"\bvnet\b|\bnsg\b": "networking", r"\bmonitor\b": "monitoring", r"\bsentinel\b": "sentinel",
    r"\bpower bi\b": "power-bi", r"\bdax\b": "dax", r"\bdata modeling\b": "data-modeling"
}

def infer_tags(text_content: str, title: str) -> str:
    tags = set()
    if title:
        m = re.search(r"\b([a-z]{1,3}-\d{2,4})\b", title.lower())
        if m: tags.add(m.group(1).upper())
    content = text_content.lower()
    for pat, tag in KEYWORD_TAG_MAP.items():
        if re.search(pat, content): tags.add(tag)
    return ",".join(list(tags)[:8]) 

# --------------------- HOTSPOT LOGIC ---------------------

def detect_hotspot_variant(q_text, options_str):
    """Determines if a hotspot is Click, Yes/No, or Dropdown."""
    q_lower = q_text.lower()
    opt_lower = options_str.lower()
    
    if "[slot" in q_lower or "[slot" in opt_lower:
        return "dropdown"
    
    if "select yes" in q_lower or "true or false" in q_lower:
        return "yes_no_matrix"
    
    # Default to Click Region (Image Map)
    return "click_region"

# --------------------- PARSERS ---------------------

def parse_options_v2(question_key, q_type, variant, options_str, correct_str):
    options_rows = []
    
    options_str = clean_text(options_str)
    correct_str = clean_text(correct_str)
    
    if not options_str: return []

    # Regex to split "A) Text"
    if re.search(r"\b[A-Za-z]\)", options_str):
        raw_options = re.split(r";\s*(?=[A-Za-z]\))", options_str)
    else:
        raw_options = options_str.split(';')

    correct_letters = set(re.findall(r"\b([A-Za-z])\)", correct_str))
    
    for idx, opt_raw in enumerate(raw_options, 1):
        opt_text = opt_raw.strip()
        
        # Strip Letter Prefix
        match = re.match(r"^([A-Za-z])\)\s*(.*)", opt_text)
        if match:
            letter = match.group(1).upper()
            text_body = match.group(2)
        else:
            letter = chr(64 + idx)
            text_body = opt_text
        
        is_correct = False
        
        # Correctness Logic
        if q_type == 'drag_drop':
            # For sequence, if it exists in correct string, it's a valid item
            if text_body in correct_str: is_correct = True
        else:
            # For MCQ/Hotspot
            if letter in correct_letters: is_correct = True
            elif text_body in correct_str and len(text_body) > 1: is_correct = True

        # --- METADATA GENERATION ---
        metadata_json = None
        
        if q_type == 'hotspot':
            if variant == 'yes_no_matrix':
                # For Matrix: Text is the statement. Correctness = Yes/No.
                metadata_json = json.dumps({
                    "variant": "yes_no_matrix",
                    "correctValue": "yes" if is_correct else "no"
                })
                is_correct = True # Row must exist
                
            elif variant == 'dropdown':
                # Special parsing for Dropdown: "A) [SLOT1] Label | Choice1, Choice2"
                # If pure text, we default to basic structure
                metadata_json = json.dumps({
                    "slotId": f"SLOT{idx}",
                    "label": f"Option {idx}",
                    "choices": [text_body], # In a real scenario, we'd extract distractors
                    "correctChoice": text_body
                })
                is_correct = True
                
            else:
                # Default: Click Region
                # We put dummy coords so it imports. User draws box in UI.
                metadata_json = json.dumps({
                    "variant": "click_region",
                    "shape": "rect",
                    "coords": {"x": 10, "y": 10 + (idx*10), "width": 50, "height": 50}
                })

        row = {
            "QuestionKey": question_key,
            "Text": text_body,
            "IsCorrect": is_correct,
            "OrderIndex": idx,
            "CorrectOrder": idx if q_type == 'drag_drop' else None,
            "Metadata": metadata_json
        }
        options_rows.append(row)
        
    return options_rows

# --------------------- MAIN ---------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--collection', required=False)
    parser.add_argument('--lookup', required=False)
    args = parser.parse_args()

    try:
        df = pd.read_excel(args.input)
        df = normalize_columns(df)
    except Exception as e:
        print(f"Error: {e}")
        return

    # Load Image Lookup
    image_lookup = {}
    if args.lookup:
        try:
            with open(args.lookup, 'r') as f: image_lookup = json.load(f)
        except: pass

    # Containers
    tbl_questions = []
    tbl_options = []
    tbl_scenarios = []
    tbl_hints = []
    tbl_quizzes = {}
    tbl_collections = {}
    tbl_categories = {}
    
    seen_scenarios = {}
    quiz_counters = {}

    for idx, row in df.iterrows():
        # Metadata Setup
        cat_name = clean_text(row.get('Category')) or DEFAULT_CATEGORY_NAME
        col_name = clean_text(row.get('Collection')) or args.collection or DEFAULT_COLLECTION_NAME
        quiz_title = clean_text(row.get('Quiz')) or f"{col_name} - Batch 1"
        
        cat_key = make_key("CAT", cat_name)
        col_key = make_key("COL", col_name)
        quiz_key = make_key("QUIZ", quiz_title)

        if cat_key not in tbl_categories:
            tbl_categories[cat_key] = {"CategoryKey": cat_key, "Name": cat_name, "Description": "", "Icon": "server", "Color": "#3B82F6", "IsActive": True}
        if col_key not in tbl_collections:
            tbl_collections[col_key] = {"CollectionKey": col_key, "Name": col_name, "CategoryKey": cat_key, "Difficulty": "medium", "IsPublic": True, "InstructorName": DEFAULT_INSTRUCTOR}
        if quiz_key not in tbl_quizzes:
            tags = infer_tags(str(row.get('Question')), quiz_title)
            tbl_quizzes[quiz_key] = {"QuizKey": quiz_key, "Title": quiz_title, "CollectionKey": col_key, "PassMark": DEFAULT_PASSMARK, "IsPublic": True, "Tags": tags}

        # Question Setup
        quiz_counters.setdefault(quiz_key, 0)
        quiz_counters[quiz_key] += 1
        q_key = f"Q-{quiz_key}-{quiz_counters[quiz_key]:03d}"
        q_type = clean_text(row.get('Question_Type', 'multiple_choice')).lower()
        
        # --- NEW: DETERMINE VARIANT ---
        q_variant = None
        if q_type == 'hotspot':
            q_variant = detect_hotspot_variant(str(row.get('Question')), str(row.get('Options')))
        
        # Scenario
        scenario_key = None
        scen_text = clean_text(row.get('Scenario'))
        if scen_text and len(scen_text) > 15:
            scen_hash = hashlib.md5(scen_text.encode()).hexdigest()
            if scen_hash in seen_scenarios:
                scenario_key = seen_scenarios[scen_hash]
            else:
                scenario_key = make_key("SCN", scen_hash)
                seen_scenarios[scen_hash] = scenario_key
                tbl_scenarios.append({
                    "ScenarioKey": scenario_key, "QuizKey": quiz_key, "Title": f"Case Study {len(seen_scenarios)}",
                    "Context": scen_text, "MediaUrl": "", "MediaType": "text", "TimeDuration": 600, "Order": len(seen_scenarios)
                })

        # Image Logic
        media_val = ""
        q_text = clean_text(row.get('Question'))
        if q_text in image_lookup: media_val = image_lookup[q_text]
        elif str(row.get('has_image')).lower() in ['true', '1', 'yes']: media_val = "1"

        tbl_questions.append({
            "QuestionKey": q_key, "QuizKey": quiz_key, "Type": q_type, 
            "Variant": q_variant, # <--- NEW COLUMN
            "Text": q_text,
            "Explanation": clean_text(row.get('Explanation')), "Points": DEFAULT_POINTS,
            "Order": quiz_counters[quiz_key], "ScenarioKey": scenario_key, "ScenarioOrder": 1 if scenario_key else None,
            "CorrectAnswer": clean_text(row.get('Correct_Options')), "PartialScoring": q_type in ['multiple_answer', 'drag_drop'],
            "MediaUrl": media_val
        })

        # Options Parsing
        opt_rows = parse_options_v2(q_key, q_type, q_variant, row.get('Options'), row.get('Correct_Options'))
        tbl_options.extend(opt_rows)

        # Hints
        if row.get('Hints'):
            tbl_hints.append({"QuestionKey": q_key, "HintText": clean_text(row.get('Hints')), "HintOrder": 1, "PointsDeduction": 0})

    # Export
    with pd.ExcelWriter(args.output, engine='openpyxl') as writer:
        pd.DataFrame(list(tbl_categories.values())).to_excel(writer, "Categories", index=False)
        pd.DataFrame(list(tbl_collections.values())).to_excel(writer, "Collections", index=False)
        pd.DataFrame(list(tbl_quizzes.values())).to_excel(writer, "Quizzes", index=False)
        pd.DataFrame(tbl_scenarios).to_excel(writer, "Scenarios", index=False)
        pd.DataFrame(tbl_questions).to_excel(writer, "Questions", index=False)
        pd.DataFrame(tbl_options).to_excel(writer, "Options", index=False)
        pd.DataFrame(tbl_hints).to_excel(writer, "Hints", index=False)

    print(f"V2.1 Transformation Complete: {len(tbl_questions)} questions processed.")

if __name__ == "__main__":
    main()
