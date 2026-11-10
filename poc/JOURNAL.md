# POC

## Milestone

### 2026-11-10

- The memory crash: holding 11 million rows in Python crashed it, and writing in batches to one Parquet file fixed it.
- The BGL skipped lines: 34,470 lines had an empty message, and the len(p) == 10 check was dropping them. Also note that a FATAL line can be labeled normal, so the level is not the label.
- The HDFS column bug: the tuple had four values and none was the message, but because the schema also had four names, Polars stored the blocks under content without any error.
