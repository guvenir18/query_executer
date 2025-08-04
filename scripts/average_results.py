import json
import glob
import csv

# Load all JSON files
files = glob.glob("input/*.json")
all_data = [json.load(open(f)) for f in files]

num_entries = len(all_data[0])
averaged_entries = []

for i in range(num_entries):
    runtimes = [d[i]["runtime"] for d in all_data]
    avg_runtime = sum(runtimes) / len(runtimes)

    base = all_data[0][i]
    averaged_entries.append({
        "val_1": base["val_1"],
        "rows_1": base["rows_1"],
        "val_2": base["val_2"],
        "rows_2": base["rows_2"],
        "val_3": base["val_3"],
        "rows_3": base["rows_3"],
        "runtime": avg_runtime
    })

# Determine dynamic header based on fields that are not empty
headers = []
sample = averaged_entries[0]
if sample["val_1"]:
    headers += ["val_1", "rows_1"]
if sample["val_2"]:
    headers += ["val_2", "rows_2"]
if sample["val_3"]:
    headers += ["val_3", "rows_3"]
headers.append("runtime")

# Write to CSV
with open("averaged_results.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile, delimiter=';')
    writer.writerow(headers)
    for row in averaged_entries:
        line = []
        if row["val_1"]:
            line += [row["val_1"], row["rows_1"]]
        if row["val_2"]:
            line += [row["val_2"], row["rows_2"]]
        if row["val_3"]:
            line += [row["val_3"], row["rows_3"]]
        line.append(round(row["runtime"], 3))
        writer.writerow(line)

print(f"✅ Created averaged_results.csv with {len(averaged_entries)} rows.")
