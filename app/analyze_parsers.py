import re

def parse_analyze_mysql(plan_text: str, filter_vars: list):
    pattern = re.compile(
        r"Filter: \((.*?)\).*?\(actual time=(.*?) rows=([^\s]+) loops=([^\)]+)\)",
        re.DOTALL
    )

    results = []
    for condition, time_range, rows, loops in pattern.findall(plan_text):
        condition_lower = condition.lower()
        for var in filter_vars:
            if var.lower() in condition_lower:
                start_time, *end_time = time_range.strip().split("..")
                start_time = float(start_time)
                end_time = float(end_time[0]) if end_time else start_time

                total_rows = int(float(rows) * float(loops))

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