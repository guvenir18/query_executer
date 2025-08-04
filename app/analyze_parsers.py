import re


def parse_analyze_mysql(plan_text: str, filter_vars: list):
    # Updated regex to also match filters with (never executed)
    pattern = re.compile(
        r"Filter: \((.*?)\)\s*\((?:(?:actual time=(.*?) rows=([^\s]+) loops=([^\)]+))|never executed)\)",
        re.DOTALL
    )

    results = []
    for match in pattern.findall(plan_text):
        condition = match[0]
        time_range = match[1]
        rows = match[2]
        loops = match[3]

        condition_lower = condition.lower()
        for var in filter_vars:
            if var.lower() in condition_lower:
                if time_range:  # Means it was executed
                    total_rows = int(float(rows) * float(loops))
                else:
                    total_rows = 0

                results.append({
                    "variable": var,
                    "total_rows": total_rows,
                })
                break

    return results


def extract_total_runtime(plan_text: str) -> float:
    """
    Extracts the total runtime (in milliseconds) from the top-level node
    in MySQL EXPLAIN ANALYZE output by taking the right-hand value
    """
    match = re.search(r"actual time=\d+\.?\d*\.\.(\d+\.?\d*)", plan_text)
    if match:
        return float(match.group(1))
    return 0.0