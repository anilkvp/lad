import re
from datetime import datetime
import pyarrow.parquet as pq
import polars as pl

raw_path = "data/raw/HDFS_v1/HDFS.log"
label_path = "data/raw/HDFS_v1/preprocessed/anomaly_label.csv"
output_file = "data/parquet/hdfs.parquet"
# scripts/load_hdfs.py: line = date time pid level component: content
BLK = re.compile(r"blk_-?\d+")

BATCH_SIZE = 1_000_000


writer = None
rows = []
skipped_lines = 0
with open(raw_path, errors="replace") as f:
    for i, line in enumerate(f):
        p = line.rstrip("\n").split(maxsplit=5)
        if len(p) == 6:
            ts = datetime.strptime(p[0] + p[1], "%y%m%d%H%M%S")
            blocks = BLK.findall(p[5])  # one line can mention several blocks
            rows.append((i, ts, p[3], p[5], blocks))
        else:
            skipped_lines += 1

        if len(rows) == BATCH_SIZE:
            table = pl.DataFrame(
                rows, schema=["line", "ts", "level", "content", "blocks"], orient="row"
            ).to_arrow()
            if writer == None:
                writer = pq.ParquetWriter(output_file, table.schema)
            writer.write(table)
            rows = []
            print(f"{i + 1} done")

if rows:
    table = pl.DataFrame(
        rows, schema=["line", "ts", "level", "content", "blocks"], orient="row"
    ).to_arrow()
    if writer == None:
        writer = pq.ParquetWriter(output_file, table.schema)
    writer.write(table)
    rows = []
if writer != None:
    writer.close()
print("**** Write completed ****")
print(f"Skipped lines :{skipped_lines}")

pl.read_csv(label_path).select(
    block="BlockId", is_anom=pl.col("Label") == "Anomaly"
).write_parquet("data/parquet/hdfs_labels.parquet")
