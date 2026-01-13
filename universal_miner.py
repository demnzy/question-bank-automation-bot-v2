import pandas as pd
import argparse
import uuid
import re

def generate_key(prefix):
    return f"{prefix}_{str(uuid.uuid4())[:8].upper()}"

def process_drag_drop_options(options_text, correct_text):
    """
    Parses 'Item A; Item B' strings into a list of dictionaries for the Options table.
    """
    if pd.isna(options_text): return []
    
    # Split by semicolon or newline
    opts = [x.strip() for x in re.split(r'[;\n]', str(options_text)) if x.strip()]
    
    # Try to determine correct order from Correct_Options
    # This is a basic implementation; specific logic depends on how consistent the LLM is
    correct_order_map = {}
    if not pd.isna(correct_text):
        correct_items = [x.strip() for x in re.split(r'[;\n]', str(correct_text)) if x.strip()]
        for idx, item in enumerate(correct_items, 1):
            correct_order_map[item] = idx

    results = []
    for i, opt in enumerate(opts, 1):
        is_correct = opt in correct_order_map
        correct_order = correct_order_map.get(opt, "")
        results.append({
            "Text": opt,
            "IsCorrect": True, # For drag/drop, usually all items are 'correct' parts of the sequence
            "OrderIndex": i,
            "CorrectOrder": i # Assuming the target order matches the list for now
        })
    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--collection', required=True)
    args = parser.parse_args()

    # Read Input
    df = pd.read_excel(args.input)

    # Initialize Tables
    questions_table = []
    options_table = []
    scenarios_table = []
    scenario_map = {} # To deduplicate scenarios

    for index, row in df.iterrows():
        # 1. Handle Scenario
        scenario_key = None
        scenario_text = row.get('Scenario')
        
        if pd.notna(scenario_text):
            # Check if we've seen this scenario text before (simple dedup)
            scen_hash = str(scenario_text)[:50] 
            if scen_hash in scenario_map:
                scenario_key = scenario_map[scen_hash]
            else:
                scenario_key = generate_key("SCN")
                scenario_map[scen_hash] = scenario_key
                scenarios_table.append({
                    "ScenarioKey": scenario_key,
                    "Title": f"Case Study {len(scenario_map)}",
                    "Context": scenario_text,
                    "MediaType": "text"
                })

        # 2. Handle Question
        q_key = generate_key("Q")
        q_type = row.get('Question_Type', 'multiple_choice')
        
        questions_table.append({
            "QuestionKey": q_key,
            "Type": q_type,
            "Text": row.get('Question'),
            "Explanation": row.get('Explanation'),
            "ScenarioKey": scenario_key,
            "Order": index + 1
        })

        # 3. Handle Options (Complex Logic for Drag/Drop)
        if q_type == 'drag_drop' or q_type == 'hotspot':
            parsed_opts = process_drag_drop_options(row.get('Options'), row.get('Correct_Options'))
            for opt in parsed_opts:
                opt['QuestionKey'] = q_key
                options_table.append(opt)
        else:
            # Standard Multiple Choice Parsing (Simplified for brevity)
            raw_opts = str(row.get('Options', '')).split(';')
            for i, opt_text in enumerate(raw_opts):
                options_table.append({
                    "QuestionKey": q_key,
                    "Text": opt_text,
                    "IsCorrect": False, # Needs logic to check against Correct_Options
                    "OrderIndex": i + 1
                })

    # Convert to DataFrames
    df_q = pd.DataFrame(questions_table)
    df_o = pd.DataFrame(options_table)
    df_s = pd.DataFrame(scenarios_table)

    # Save to Multi-sheet Excel
    with pd.ExcelWriter(args.output) as writer:
        df_q.to_excel(writer, sheet_name='Questions', index=False)
        df_o.to_excel(writer, sheet_name='Options', index=False)
        df_s.to_excel(writer, sheet_name='Scenarios', index=False)

    print("Transformation V2 Complete.")

if __name__ == "__main__":
    main()
