#!/usr/bin/env python3
"""
universal_miner.py (V2 Hybrid Edition)

Merges V1 robustness (Tagging, Regex, Metadata) with V2 Relational Schema (Scenarios, Options Table).
"""

import argparse
import re
import json
import pandas as pd
import uuid
import hashlib
from pathlib import Path
from typing import Dict, List, Set, Any

# --- CONFIGURATION ---
DEFAULT_CATEGORY_NAME = "IT & Technology"
DEFAULT_COLLECTION_NAME = "General Certification"
DEFAULT_PASSMARK = 70
DEFAULT_POINTS = 1
DEFAULT_INSTRUCTOR = "Demo Instructor"

# --------------------- HELPERS (From V1) ---------------------

def slugify(text: str) -> str:
    tokens = re.sub(r"[^A-Za-z0-9]+", " ", str(text)).strip().split()
    return "-".join(t.upper() for t in tokens)

def make_key(prefix: str, base: str) -> str:
    # Deterministic key generation based on content
    if not base: return f"{prefix}_{str(uuid.uuid4())[:8].upper()}"
    clean = re.sub(r"[^A-Za-z0-9]", "", str(base))
    return f"{prefix}_{clean[:15].upper()}_{hashlib.md5(str(base).encode()).hexdigest()[:4].upper()}"

def sentence_case_name(name: str) -> str:
    return str(name).strip().title() if name else ""

def clean_hint_text(h: str) -> str:
    if pd.isna(h) or h is None: return ""
    s = str(h).strip()
    s = re.sub(r'^\s*hint\s*:\s*', '', s, flags=re.IGNORECASE)
    return s

def clean_text(text):
    if pd.isna(text): return ""
    return str(text).strip()

# --------------------- TAGGING LOGIC (From V1) ---------------------
ROLE_HINT_WORDS = {"administrator", "admin", "developer", "security", "architect", "engineer", "data", "ai", "devops", "fundamentals", "associate", "expert"}
KEYWORD_TAG_MAP = {
    r"\b(azure\s*ad|entra)\b": "identity", r"\bconditional access\b": "conditional-access",
    r"\bmfa\b": "mfa", r"\brbac\b": "rbac", r"\bkey vault\b": "key-vault",
    r"\bmanaged identity\b": "managed-identity", r"\bpolicy\b": "policy", 
    r"\bblob\b|\bstorage account\b": "storage", r"\bcosmos db\b": "cosmosdb", r"\bsql\b": "sql",
    r"\bvirtual machine\b|\bvm\b": "compute", r"\baks\b|\bkubernetes\b": "containers",
    r"\bvnet\b|\bnsg\b": "networking", r"\bmonitor\b": "monitoring", r"\bsentinel\b": "sentinel",
    r"\bpower bi\b": "power-bi", r"\bdax\b": "dax", r"\bdata modeling\b": "data-modeling",
    r"\bvisualization\b": "visualization", r"\bpower query\b": "power-query"
}

def infer_tags(text_content: str, title: str) -> str:
    tags = set()
    # 1. Title Tokens
    if title:
        m = re.search(r"\b([a-z]{1,3}-\d{2,4})\b", title.lower())
        if m: tags.add(m.group(1).upper())
    
    # 2. Content Scan
    content = text_content.lower()
    for pat, tag in KEYWORD_TAG_MAP.items():
        if re.search(pat, content): tags.add(tag)
        
    return ",".join(list(tags)[:8]) # Limit to 8 tags

# --------------------- DATA PROCESSING ---------------------

def normalize_input(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes column names from various n8n outputs."""
    cols = {c.lower().replace(" ", "_"): c for c in df.columns}
    
    # Map common variations to standard internal names
    mapping = {
        "question": "Question",
        "options": "Options",
        "correct_options": "Correct_Options",
        "answer": "Correct_Options",
        "explanation": "Explanation",
        "hints": "Hints",
        "scenario": "Scenario",
        "question_type": "Question_Type",
        "type": "Question_Type",
        "category": "Category",
        "collection": "Collection",
        "quiz": "Quiz",
        "difficulty": "difficulty",
        "has_image": "has_image"
    }
    
    renamed = {}
    for standard, target in mapping.items():
        # Find if any case-insensitive match exists
        matches = [orig for low, orig in cols.items() if low == standard]
        if matches:
            renamed[matches[0]] = target
            
    df = df.rename(columns=renamed)
    return df

def parse_options_v2(question_key, q_type, options_str, correct_str):
    """
    Parses 'A) Text; B) Text' into relational rows.
    Handles matching Correct_Options to identify IsCorrect and CorrectOrder.
    """
    options_rows = []
    
    # 1. Clean Inputs
    options_str = clean_text(options_str)
    correct_str = clean_text(correct_str)
    
    if not options_str:
        return []

    # 2. Split Options (Semicolon or Newline)
    # Regex looks for "A) " pattern to split cleanly if present
    if re.search(r"\b[A-Za-z]\)", options_str):
        raw_options = re.split(r";\s*(?=[A-Za-z]\))", options_str)
    else:
        raw_options = options_str.split(';')

    # 3. Analyze Correct Answer
    # Extract just the letters from the correct string for matching (e.g. "A", "B")
    correct_letters = set(re.findall(r"\b([A-Za-z])\)", correct_str))
    if not correct_letters:
        # Fallback: Try to find exact text match
        correct_letters = set() # logic handled below

    # 4. Logic Switch
    is_sequence = q_type in ['drag_drop', 'sequence', 'ordering']
    
    for idx, opt_raw in enumerate(raw_options, 1):
        opt_text = opt_raw.strip()
        
        # Extract Letter Prefix (e.g., "A)")
        letter_match = re.match(r"^([A-Za-z])\)\s*(.*)", opt_text)
        
        if letter_match:
            letter = letter_match.group(1).upper()
            text_body = letter_match.group(2)
        else:
            # If no letter assigned by AI, assume sequential A, B, C
            letter = chr(64 + idx) # 1=A, 2=B
            text_body = opt_text

        # Determine Correctness
        is_correct = False
        correct_order = None
        
        if is_sequence:
            # For drag drop, usually ALL options are correct parts of the sequence.
            # We need to find the order.
            # If correct_str is "A) Step 1; B) Step 2", we assume that IS the order.
            # Simple Logic: If AI output Correct_Options as a list, find this item's position in it.
            if opt_text in correct_str:
                is_correct = True
                # Try to find position
                # This is complex without perfect AI output, defaulting to Input Order for now
                correct_order = idx 
        else:
            # Standard MCQ / Multiple Answer
            if letter in correct_letters:
                is_correct = True
            elif text_body in correct_str:
                is_correct = True

        row = {
            "QuestionKey": question_key,
            "Text": text_body,
            "IsCorrect": is_correct,
            "OrderIndex": idx,
            "CorrectOrder": correct_order if is_sequence else None,
            "HotspotCoords": None
        }
        options_rows.append(row)
        
    return options_rows

# --------------------- MAIN BUILDER ---------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help="Path to Enriched Excel")
    parser.add_argument('--output', required=True, help="Output Filename")
    parser.add_argument('--collection', required=True, help="Collection Name override")
    parser.add_argument('--lookup', required=False, help="Image URL Lookup JSON (Optional)")
    args = parser.parse_args()

    # 1. Load Data
    try:
        df = pd.read_excel(args.input)
        df = normalize_input(df)
    except Exception as e:
        print(f"CRITICAL: Failed to load input file. {e}")
        return

    # 2. Containers for V2 Schema
    tbl_questions = []
    tbl_options = []
    tbl_scenarios = []
    tbl_hints = []
    
    # Metadata Containers
    tbl_quizzes = {}
    tbl_collections = {}
    tbl_categories = {}

    # 3. Trackers
    seen_scenarios = {} # {hash: key}
    
    # 4. Processing Loop
    for index, row in df.iterrows():
        
        # --- A. HIERARCHY (Category -> Collection -> Quiz) ---
        cat_name = clean_text(row.get('Category', DEFAULT_CATEGORY_NAME))
        col_name = clean_text(row.get('Collection', args.collection))
        quiz_title = clean_text(row.get('Quiz', f"{col_name} Batch 1"))
        
        cat_key = make_key("CAT", cat_name)
        col_key = make_key("COL", col_name)
        quiz_key = make_key("QUIZ", quiz_title)

        # Populate Metadata Tables (Deduplicated by dict key)
        if cat_key not in tbl_categories:
            tbl_categories[cat_key] = {
                "CategoryKey": cat_key, "Name": cat_name, 
                "Description": f"{cat_name} Certification", "Icon": "server", "Color": "#3B82F6", "IsActive": True
            }
            
        if col_key not in tbl_collections:
            tbl_collections[col_key] = {
                "CollectionKey": col_key, "Name": col_name, 
                "Description": f"Preparation for {col_name}", "CategoryKey": cat_key,
                "Difficulty": "medium", "IsPublic": True, "InstructorName": DEFAULT_INSTRUCTOR
            }
            
        if quiz_key not in tbl_quizzes:
            # Infer tags for this quiz based on first few rows
            tags = infer_tags(str(row.get('Question')) + " " + str(row.get('Explanation')), quiz_title)
            tbl_quizzes[quiz_key] = {
                "QuizKey": quiz_key, "Title": quiz_title, 
                "Description": f"Practice questions for {quiz_title}", "CollectionKey": col_key,
                "Difficulty": "medium", "PassMark": DEFAULT_PASSMARK, "TimeLimitSeconds": 3600,
                "IsPublic": True, "Tags": tags
            }

        # --- B. SCENARIO HANDLING ---
        scenario_key = None
        scen_text = clean_text(row.get('Scenario'))
        
        # Valid scenario check: must be longer than "Topic 1" (e.g. > 15 chars)
        if scen_text and len(scen_text) > 15:
            scen_hash = hashlib.md5(scen_text.encode()).hexdigest()
            if scen_hash in seen_scenarios:
                scenario_key = seen_scenarios[scen_hash]
            else:
                scenario_key = make_key("SCN", scen_hash)
                seen_scenarios[scen_hash] = scenario_key
                tbl_scenarios.append({
                    "ScenarioKey": scenario_key,
                    "QuizKey": quiz_key,
                    "Title": f"Case Study {len(seen_scenarios)}",
                    "Context": scen_text,
                    "MediaUrl": "", "MediaType": "text", "TimeDuration": 600,
                    "Order": len(seen_scenarios)
                })

        # --- C. QUESTION ---
        q_text = clean_text(row.get('Question'))
        q_key = make_key("Q", f"{quiz_key}_{index}_{q_text[:10]}")
        q_type = clean_text(row.get('Question_Type', 'multiple_choice')).lower()
        
        # Mapping Types to Schema
        valid_types = ["multiple_choice", "multiple_answer", "true_false", "short_answer", "drag_drop", "hotspot"]
        if q_type not in valid_types: q_type = "multiple_choice"

        tbl_questions.append({
            "QuestionKey": q_key,
            "QuizKey": quiz_key,
            "Type": q_type,
            "Text": q_text,
            "Explanation": clean_text(row.get('Explanation')),
            "Points": DEFAULT_POINTS,
            "Order": index + 1,
            "ScenarioKey": scenario_key,
            "ScenarioOrder": 1 if scenario_key else None,
            "CorrectAnswer": clean_text(row.get('Correct_Options')), # Fallback text
            "FuzzyMatch": False,
            "PartialScoring": q_type in ['multiple_answer', 'drag_drop']
        })

        # --- D. OPTIONS ---
        # Special Parser for Hybrid Types
        opt_rows = parse_options_v2(
            q_key, q_type, 
            row.get('Options'), 
            row.get('Correct_Options')
        )
        tbl_options.extend(opt_rows)

        # --- E. HINTS ---
        hint_text = clean_hint_text(row.get('Hints'))
        if hint_text:
            tbl_hints.append({
                "QuestionKey": q_key,
                "HintText": hint_text,
                "HintOrder": 1,
                "PointsDeduction": 0
            })

    # 5. Export to Excel
    print(f"Compiling V2 Export... {len(tbl_questions)} Questions found.")
    
    with pd.ExcelWriter(args.output, engine='openpyxl') as writer:
        pd.DataFrame(list(tbl_categories.values())).to_excel(writer, "Categories", index=False)
        pd.DataFrame(list(tbl_collections.values())).to_excel(writer, "Collections", index=False)
        pd.DataFrame(list(tbl_quizzes.values())).to_excel(writer, "Quizzes", index=False)
        pd.DataFrame(tbl_scenarios).to_excel(writer, "Scenarios", index=False)
        pd.DataFrame(tbl_questions).to_excel(writer, "Questions", index=False)
        pd.DataFrame(tbl_options).to_excel(writer, "Options", index=False)
        pd.DataFrame(tbl_hints).to_excel(writer, "Hints", index=False)

    print(f"Success! Saved to {args.output}")

if __name__ == "__main__":
    main()
