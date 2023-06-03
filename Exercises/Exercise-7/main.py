from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import glob
import zipfile
import tempfile
import os


def getFiles(pathStr: str):
    files = glob.glob(os.getcwd() + pathStr, recursive=True)
    return files


def addFilenameColumn(df):
    source_file_df = df.withColumn("source_file", F.input_file_name())
    return source_file_df


def addFileDateColumn(df):
    file_date_df = df.withColumn(
        "file_date",
        F.to_date(F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1)),
    )

    return file_date_df


def addStorageRankingColumn(df):
    capacity_bytes_df = df.select(
        F.col("capacity_bytes").alias("secondary_capacity_bytes"), "model"
    )

    rank_window = Window.orderBy(F.col("secondary_capacity_bytes").desc())

    storage_ranking_df = capacity_bytes_df.withColumn(
        "storage_ranking", F.rank().over(rank_window)
    )

    storage_df = df.join(storage_ranking_df, ["model"], "left")

    return storage_df


def addBrandColumn(df):
    brand_df = df.withColumn(
        "brand",
        F.when(F.col("model").contains(" "), F.split(F.col("model"), " ")[0]).otherwise(
            "unknown"
        ),
    )
    return brand_df


def addPrimaryKeyColumn(df):
    unique_columns = ["serial_number", "model"]

    concatenated_columns = F.concat(
        *[F.coalesce(F.col(column), F.lit("")) for column in unique_columns]
    )

    primary_key_df = df.withColumn("primary_key", F.sha2(concatenated_columns, 256))

    return primary_key_df


def main():
    file = getFiles("/data/*.zip")[0]
    csv_file_name = file.split("/")[-1].replace(".csv.zip", "")

    with zipfile.ZipFile(file, "r") as f:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            f.extract(f"{csv_file_name}.csv", os.path.dirname(temp_path))

    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()

    df = (
        spark.read.format("csv")
        .option("header", True)
        .load(f"{os.path.dirname(temp_path)}/{csv_file_name}.csv")
    )

    source_file_df = addFilenameColumn(df)

    addFileDateColumn(source_file_df)

    df = addBrandColumn(df)

    df = addStorageRankingColumn(df)

    final_df = addPrimaryKeyColumn(df)

    final_df.show()

    os.remove(temp_path)


if __name__ == "__main__":
    main()
