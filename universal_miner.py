import pandas as pd
import argparse
import uuid
import re
import hashlib

# --- HELPERS ---
def generate_key(prefix, text_seed=None):
    """Generates a deterministic or random key."""
    if text_seed:
        hash_object = hashlib.md5(str(text_seed).encode())
        return f"{prefix}_{hash_object.hexdigest()[:8].upper()}"
    return f"{prefix}_{str(uuid.uuid4())[:8].upper()}"

def clean_text(text):
    if pd.isna(text): return ""
    return str(text).strip()

def parse_options_v2(question_key, q_type, options_str, correct_str):
    """
    Parses options string into V2 relational rows.
    Handles Multiple Choice (A/B/C) AND Drag & Drop (Sequence).
    """
    options_rows = []
    
    # 1. Clean Inputs
    raw_options = [x.strip() for x in re.split(r'[;\n]', clean_text(options_str)) if x.strip()]
    raw_correct = [x.strip() for x in re.split(r'[;\n]', clean_text(correct_str)) if x.strip()]
    
    # 2. Logic Switch based on Type
    is_sequence = q_type in ['drag_drop', 'sequence']
    
    # Create a lookup for correct answers
    # For MC: Just checks existence. For DragDrop: Index matters.
    correct_lookup = {opt: i+1 for i, opt in enumerate(raw_correct)}

    for idx, opt_text in enumerate(raw_options, 1):
        is_correct = False
        correct_order = None
        
        # Check against correct list
        # We try exact match first, then fuzzy match if needed (omitted for brevity)
        if opt_text in correct_lookup:
            is_correct = True
            if is_sequence:
                correct_order = correct_lookup[opt_text]
        
        row = {
            "QuestionKey": question_key,
            "Text": opt_text,
            "IsCorrect": is_correct,
            "OrderIndex": idx, # The order they appear in the question
            "CorrectOrder": correct_order if is_sequence else None,
            "HotspotCoords": None 
        }
        options_rows.append(row)
        
    return options_rows

# --- MAIN SCRIPT ---
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help="Input Enriched Excel")
    parser.add_argument('--output', required=True, help="Output Final Excel")
    parser.add_argument('--collection', required=True, help="Collection Name")
    args = parser.parse_args()

    # 1. Load Data
    try:
        df = pd.read_excel(args.input)
    except Exception as e:
        print(f"Error reading excel: {e}")
        return

    # 2. Initialize V2 Containers
    data_questions = []
    data_options = []
    data_scenarios = []
    data_quizzes = []
    data_collections = []
    data_categories = []

    # 3. Setup Metadata Keys
    collection_key = generate_key("COL", args.collection)
    category_key = "CAT_GENERIC" # Default
    quiz_key = generate_key("QUIZ", args.collection + "_Batch1")

    # Add Top Level Data (Collection/Quiz) - simplified for V2
    data_collections.append({
        "CollectionKey": collection_key, 
        "Name": args.collection, 
        "CategoryKey": category_key,
        "IsPublic": True
    })
    
    data_quizzes.append({
        "QuizKey": quiz_key,
        "Title": f"{args.collection} Practice",
        "CollectionKey": collection_key,
        "PassMark": 70,
        "IsPublic": True
    })

    # 4. Processing Loop
    scenario_tracker = {} # To deduplicate scenarios

    for index, row in df.iterrows():
        # --- A. SCENARIOS ---
        scenario_key = None
        scen_text = clean_text(row.get('Scenario'))
        
        if scen_text and len(scen_text) > 10: # Min length to be real
            # Deduplicate based on content hash
            scen_hash = hashlib.md5(scen_text.encode()).hexdigest()
            
            if scen_hash in scenario_tracker:
                scenario_key = scenario_tracker[scen_hash]
            else:
                scenario_key = generate_key("SCN", scen_hash)
                scenario_tracker[scen_hash] = scenario_key
                
                data_scenarios.append({
                    "ScenarioKey": scenario_key,
                    "QuizKey": quiz_key,
                    "Title": f"Case Study {len(scenario_tracker)}",
                    "Context": scen_text,
                    "Order": len(scenario_tracker)
                })

        # --- B. QUESTIONS ---
        q_key = generate_key("Q", f"{quiz_key}_{index}")
        q_type = str(row.get('Question_Type', 'multiple_choice')).lower()
        
        data_questions.append({
            "QuestionKey": q_key,
            "QuizKey": quiz_key,
            "Type": q_type,
            "Text": clean_text(row.get('Question')),
            "Explanation": clean_text(row.get('Explanation')),
            "ScenarioKey": scenario_key,
            "Order": index + 1,
            "Points": 1
        })

        # --- C. OPTIONS ---
        # Call the parser to handle Drag/Drop vs MCQ logic
        opts = parse_options_v2(
            q_key, 
            q_type, 
            row.get('Options'), 
            row.get('Correct_Options')
        )
        data_options.extend(opts)

    # 5. Export to Excel (Multi-Sheet)
    with pd.ExcelWriter(args.output, engine='openpyxl') as writer:
        pd.DataFrame(data_questions).to_excel(writer, sheet_name='Questions', index=False)
        pd.DataFrame(data_options).to_excel(writer, sheet_name='Options', index=False)
        pd.DataFrame(data_scenarios).to_excel(writer, sheet_name='Scenarios', index=False)
        pd.DataFrame(data_quizzes).to_excel(writer, sheet_name='Quizzes', index=False)
        pd.DataFrame(data_collections).to_excel(writer, sheet_name='Collections', index=False)
        # pd.DataFrame(data_categories).to_excel(writer, sheet_name='Categories', index=False) 

    print(f"Successfully created V2 Export: {args.output}")
    print(f"Questions: {len(data_questions)} | Scenarios: {len(data_scenarios)}")

if __name__ == "__main__":
    main()
