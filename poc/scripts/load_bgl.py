# scripts/load_bgl.py: BGL line = label epoch date node time node type comp level content
import polars as pl

raw_path = "data/raw/BGL/BGL.log"

rows = []
skipped_lines = 0
with open(raw_path, errors="replace") as f:
    for i, line in enumerate(f):
        p = line.rstrip("\n").split(maxsplit=9)
        if len(p) >= 9:
            content = p[9] if len(p) == 10 else ""
            rows.append((i, p[0] != "-", int(p[1]), p[3], content))
        else:
            skipped_lines += 1
pl.DataFrame(
    rows, schema=["line", "is_anom", "ts", "node", "content"], orient="row"
).write_parquet("data/parquet/bgl.parquet")
print("Write completed\n")
print(f"Skipped lines: {skipped_lines}")
