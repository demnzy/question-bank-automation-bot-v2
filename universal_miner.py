#!/usr/bin/env python3
"""
universal_miner.py (V2.3 - Application Schema Aligned + Anti-Hallucination)

Updates:
- Sanitization: Added null eradicator, overlap deduplication, and hallucination filter.
- Scenarios: Fuzzy fingerprinting to prevent duplicates; strict validation to reject false scenarios.
- Options: Exact alignment with demo-all-types schema (IsCorrect boolean for Matrix, JSON Metadata for Dropdowns, CorrectOrder for Drag & Drop).
- Questions: Added Hotspot_Variant extraction directly from LLM output.
"""

import argparse
import re
import json
import pandas as pd
import uuid
import hashlib

# --- CONFIGURATION ---
DEFAULT_CATEGORY_NAME = "IT & Technology"
DEFAULT_COLLECTION_NAME = "General Certification"
DEFAULT_PASSMARK = 70
DEFAULT_POINTS = 1
DEFAULT_INSTRUCTOR = "Demo Instructor"

# --------------------- HELPERS ---------------------

def generate_robust_fingerprint(text: str) -> str:
    """Creates a collision-resistant hash ignoring whitespace, case, and punctuation."""
    if not text or pd.isna(text): return ""
    # Strip everything except alphanumeric characters
    normalized = re.sub(r'[\W_]+', '', str(text).lower())
    return hashlib.md5(normalized.encode()).hexdigest()

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

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes input column names."""
    cols = {c.lower().replace(" ", "_").replace(".", ""): c for c in df.columns}
    mapping = {
        "question": "Question", "options": "Options", 
        "correct_options": "Correct_Options", "answers": "Correct_Options",
        "explanation": "Explanation", "hints": "Hints", "scenario": "Scenario",
        "question_type": "Question_Type", "type": "Question_Type",
        "hotspot_variant": "Hotspot_Variant", "variant": "Hotspot_Variant",
        "category": "Category", "collection": "Collection", "quiz": "Quiz",
        "difficulty": "difficulty", "has_image": "has_image"
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
    r"\bvnet\b|\bnsg\b": "networking", r"\bmonitor\b": "monitoring"
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
    """Determines if a hotspot is Click, Yes/No, or Dropdown if LLM fails to provide it."""
    q_lower = q_text.lower()
    opt_lower = options_str.lower()
    
    if "[slot" in q_lower or "[slot" in opt_lower: return "dropdown"
    if "select yes" in q_lower or "true or false" in q_lower: return "yes_no_matrix"
    return "click_region"

# --------------------- PARSERS ---------------------

def parse_options_v2(question_key, q_type, variant, options_str, correct_str):
    options_rows = []
    
    options_str = clean_text(options_str)
    correct_str = clean_text(correct_str)
    
    if not options_str: return []

    # Regex to split items safely
    if re.search(r"\b[A-Za-z]\)", options_str):
        raw_options = re.split(r";\s*(?=[A-Za-z]\))", options_str)
    else:
        raw_options = options_str.split(';')

    correct_letters = set(re.findall(r"\b([A-Za-z])\)", correct_str))
    correct_sequence = [clean_text(x) for x in correct_str.split(';')]
    
    for idx, opt_raw in enumerate(raw_options, 1):
        opt_text = opt_raw.strip()
        if not opt_text: continue
        
        # Strip Letter Prefix if exists
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
        
        # --- TYPE SPECIFIC LOGIC ---
        if q_type == 'drag_drop':
            # Drag & Drop: Use CorrectOrder column
            # Check where this option appears in the correct string sequence
            for seq_idx, correct_item in enumerate(correct_sequence, 1):
                if text_body.lower() in correct_item.lower():
                    correct_order = seq_idx
                    break
            is_correct = False # Not strictly "True/False", relies on CorrectOrder

        elif q_type == 'hotspot' and variant == 'yes_no_matrix':
            # Yes/No Matrix: IsCorrect is True if the statement evaluates to Yes
            is_correct = False
            for correct_item in correct_sequence:
                if text_body.lower() in correct_item.lower() and "yes" in correct_item.lower():
                    is_correct = True
                    break

        elif q_type == 'hotspot' and variant == 'dropdown':
            # Dropdown: Extract SLOT, Choices, and CorrectChoice
            match_slot = re.search(r"\[(SLOT\d+)\]", opt_text, re.IGNORECASE)
            slot_id = match_slot.group(1).upper() if match_slot else f"SLOT{idx}"
            
            raw_choices = re.sub(r"\[SLOT\d+\]", "", opt_text).strip()
            choices_list = [c.strip() for c in re.split(r"[,|]", raw_choices) if c.strip()]
            
            # Find the correct choice for this specific slot
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
            is_correct = True # The row itself is valid and must render
            text_body = slot_id # The Text field typically holds the Slot ID in the DB

        elif q_type == 'hotspot' and variant == 'click_region':
            # Click Region: Output dummy bounding box for frontend interaction
            metadata_json = json.dumps({
                "variant": "click_region",
                "shape": "rect",
                "coords": {"x": 10, "y": 10 + (idx*10), "width": 50, "height": 50}
            })
            is_correct = True if letter in correct_letters or text_body in correct_str else False

        else:
            # Standard Multiple Choice / Multiple Answer
            if letter in correct_letters: is_correct = True
            elif text_body in correct_str and len(text_body) > 1: is_correct = True

        row = {
            "QuestionKey": question_key,
            "Text": text_body,
            "IsCorrect": is_correct,
            "OrderIndex": idx,
            "Metadata": metadata_json,
            "CorrectOrder": correct_order
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
        
        # --- 1. THE NULL ERADICATOR ---
        df = df.replace(to_replace=r'(?i)^null$', value='', regex=True)
        
        # --- 2. OVERLAP DEDUPLICATION ---
        df['QuestionHash'] = df['Question'].apply(lambda x: generate_robust_fingerprint(str(x)))
        original_count = len(df)
        df = df.drop_duplicates(subset=['QuestionHash'], keep='first')
        print(f"Dropped {original_count - len(df)} duplicate questions caused by chunk overlap.")
        
        # --- 3. THE HALLUCINATION FILTER ---
        df = df.dropna(subset=['Options'])
        df = df[df['Options'].str.strip() != '']
        
    except Exception as e:
        print(f"Error reading or sanitizing file: {e}")
        return

    # Load Image Lookup if provided
    image_lookup = {}
    if args.lookup:
        try:
            with open(args.lookup, 'r') as f: image_lookup = json.load(f)
        except: pass

    # Containers
    tbl_questions, tbl_options, tbl_scenarios = [], [], []
    tbl_quizzes, tbl_collections, tbl_categories = {}, {}, {}
    seen_scenarios = {}
    quiz_counters = {}

    for idx, row in df.iterrows():
        # --- METADATA ---
        cat_name = clean_text(row.get('Category')) or DEFAULT_CATEGORY_NAME
        col_name = clean_text(row.get('Collection')) or args.collection or DEFAULT_COLLECTION_NAME
        quiz_title = clean_text(row.get('Quiz')) or f"{col_name} - Batch 1"
        
        cat_key = make_key("CAT", cat_name)
        col_key = make_key("COL", col_name)
        quiz_key = make_key("QUIZ", quiz_title)

        if cat_key not in tbl_categories:
            tbl_categories[cat_key] = {"CategoryKey": cat_key, "Name": cat_name, "Description": "", "IsActive": True}
        if col_key not in tbl_collections:
            tbl_collections[col_key] = {"CollectionKey": col_key, "Name": col_name, "CategoryKey": cat_key, "Difficulty": "medium", "IsPublic": True}
        if quiz_key not in tbl_quizzes:
            tags = infer_tags(str(row.get('Question')), quiz_title)
            tbl_quizzes[quiz_key] = {"QuizKey": quiz_key, "Title": quiz_title, "CollectionKey": col_key, "PassMark": DEFAULT_PASSMARK, "IsPublic": True, "Tags": tags}

        # --- QUESTION SETUP ---
        quiz_counters.setdefault(quiz_key, 0)
        quiz_counters[quiz_key] += 1
        q_key = f"Q-{quiz_key}-{quiz_counters[quiz_key]:03d}"
        q_type = clean_text(row.get('Question_Type', 'multiple_choice')).lower()
        q_text = clean_text(row.get('Question'))
        
        # Determine Variant
        q_variant = clean_text(row.get('Hotspot_Variant', ''))
        if q_variant.lower() == 'null' or not q_variant:
            q_variant = detect_hotspot_variant(q_text, str(row.get('Options'))) if q_type == 'hotspot' else None

        # --- STRICT SCENARIO VALIDATION ---
        scenario_key = None
        scen_text = clean_text(row.get('Scenario'))
        
        if scen_text and scen_text.lower() != 'null' and len(scen_text) > 30 and scen_text.lower() not in q_text.lower():
            scen_fingerprint = generate_robust_fingerprint(scen_text)
            
            if scen_fingerprint in seen_scenarios:
                scenario_key = seen_scenarios[scen_fingerprint]
            else:
                scenario_key = make_key("SCN", scen_fingerprint[:8])
                seen_scenarios[scen_fingerprint] = scenario_key
                tbl_scenarios.append({
                    "ScenarioKey": scenario_key, "QuizKey": quiz_key, "Title": f"Case Study / Exhibit {len(seen_scenarios)}",
                    "Context": scen_text
                })

        # --- IMAGE MAPPING ---
        media_val = ""
        if q_text in image_lookup: media_val = image_lookup[q_text]
        elif str(row.get('has_image')).lower() in ['true', '1', 'yes']: media_val = "1"

        tbl_questions.append({
            "QuestionKey": q_key, "QuizKey": quiz_key, "Type": q_type, 
            "Text": q_text, "Explanation": clean_text(row.get('Explanation')), "Points": DEFAULT_POINTS,
            "Order": quiz_counters[quiz_key], "ScenarioKey": scenario_key, "ScenarioOrder": 1 if scenario_key else None,
            "Hints": clean_text(row.get('Hints')), 
            "PartialScoring": True if q_type in ['multiple_answer', 'drag_drop'] else False,
            "Variant": q_variant,
            "MediaUrl": media_val
        })

        # --- OPTIONS PARSING ---
        opt_rows = parse_options_v2(q_key, q_type, q_variant, row.get('Options'), row.get('Correct_Options'))
        tbl_options.extend(opt_rows)

    # --- EXPORT TO EXCEL (MULTI-SHEET) ---
    with pd.ExcelWriter(args.output, engine='openpyxl') as writer:
        pd.DataFrame(list(tbl_categories.values())).to_excel(writer, "Categories", index=False)
        pd.DataFrame(list(tbl_collections.values())).to_excel(writer, "Collections", index=False)
        pd.DataFrame(list(tbl_quizzes.values())).to_excel(writer, "Quizzes", index=False)
        pd.DataFrame(tbl_scenarios).to_excel(writer, "Scenarios", index=False)
        pd.DataFrame(tbl_questions).to_excel(writer, "Questions", index=False)
        pd.DataFrame(tbl_options).to_excel(writer, "Options", index=False)

    print(f"V2.3 Schema Transformation Complete: {len(tbl_questions)} questions processed cleanly.")

if __name__ == "__main__":
    main()
