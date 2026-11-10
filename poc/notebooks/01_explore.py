import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    return mo, pl


@app.cell
def _(pl):
    blg = pl.read_parquet("data/parquet/bgl.parquet")
    hdfs = pl.read_parquet("data/parquet/hdfs.parquet")
    hdfs_labels = pl.read_parquet("data/parquet/hdfs_labels.parquet")

    blg.height, hdfs.height, hdfs_labels.height
    return blg, hdfs, hdfs_labels


@app.cell
def _(blg):
    blg.head()
    return


@app.cell
def _(hdfs):
    hdfs.schema["blocks"]
    return


@app.cell
def _(hdfs_labels):
    hdfs_labels.head(10)
    return


@app.cell
def _(blg, pl):
    blg.height, blg["is_anom"].sum(), blg["is_anom"].mean(), blg.filter(pl.col("content") == "").height
    return


@app.cell
def _(hdfs):
    hdfs.height, hdfs.schema
    return


@app.cell
def _(hdfs_labels):
    hdfs_labels.height, hdfs_labels["is_anom"].sum()
    return


@app.cell
def _(hdfs, pl):
    hdfs.filter(pl.col("blocks").list.len()==0).height
    return


@app.cell
def _(hdfs, hdfs_labels, pl):
    log_blocks = hdfs.select(block=pl.col("blocks").explode()).unique() 
    log_blocks.join(hdfs_labels, on="block", how="anti").height
    hdfs_labels.join(log_blocks, on="block", how="anti").height
    log_blocks
    return


@app.cell
def _(blg, hdfs, mo):
    mo.md(f"BGL has **{blg.height:,}** lines, **{blg['is_anom'].mean():.1%}** anomalous.\nHDFS has **{hdfs.height:,}** lines")

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
 
    """)
    return


if __name__ == "__main__":
    app.run()
