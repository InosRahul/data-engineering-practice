from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import zipfile
import tempfile
import glob
import os
from datetime import timedelta, datetime
from datetime import date as date_class


def getFiles(pathStr: str):
    files = glob.glob(os.getcwd() + pathStr, recursive=True)
    return files


def hasColumn(df, colName: str):
    if colName in df.columns:
        return True
    else:
        return False


def getQuarter(p_date: date_class) -> int:
    return (p_date.month - 1) // 3 + 1


def getAgeFromYear(birthYear: date_class) -> int:
    current_year = datetime.today().year
    return current_year - birthYear


def getLastDayOfQuarter(p_date: date_class):
    quarter = getQuarter(p_date)
    return datetime(
        p_date.year + 3 * quarter // 12, 3 * quarter % 12 + 1, 1
    ) + timedelta(days=-1)


def calculateAvgTripsPerDay(df, outputfilepath: str):
    avg_trips_per_day_df = df.groupBy("date").agg(
        F.avg("trip_duration").alias("avg_trip_duration")
    )
    avg_trips_per_day_df.coalesce(1).write.option("header", True).csv(
        f"{outputfilepath}/average_trips"
    )


def calcuateCountTripsPerDay(df, outputfilepath: str):
    trips_per_day_df = df.groupBy("date").agg(F.count("date").alias("trips_per_day"))

    trips_per_day_df.coalesce(1).write.option("header", True).csv(
        f"{outputfilepath}/trips_per_day"
    )


def getPopularStartingStation(df, outputfilepath: str):
    df = df.withColumnRenamed("start_station_name", "from_station_name")
    df = df.select(F.month("date").alias("month"), "from_station_name")

    count_df = df.groupBy("month", "from_station_name").agg(
        F.count("from_station_name").alias("start_station_count")
    )

    # Create a window specification to partition by month and order by the count of trips
    window_spec = Window.partitionBy("month").orderBy(
        F.col("start_station_count").desc()
    )

    # Add a rank column based on the count of trips within each month
    ranked_df = count_df.withColumn("rank", F.row_number().over(window_spec))

    # Filter the DataFrame to keep only rows with rank 1
    popular_station_df = ranked_df.filter(F.col("rank") == 1).select(
        "month", "from_station_name", "start_station_count"
    )

    popular_station_df.coalesce(1).write.option("header", True).csv(
        f"{outputfilepath}/popular_stations"
    )


def getLastTwoWeeksPopularTripStations(df, outputfilepath: str):
    df = df.withColumnRenamed("end_station_name", "to_station_name")

    # Get first date from the dataset
    first_date = df.select(F.first("date")).collect()[0][0]

    # Get the last date of the quarter
    last_date_of_quarter = getLastDayOfQuarter(first_date)

    # Get date two weeks before the last date of the quarter
    last_two_week_start_date = last_date_of_quarter - timedelta(days=14)

    count_df = (
        df.filter(F.col("date") >= last_two_week_start_date)
        .groupBy("date", "to_station_name")
        .agg(F.count("to_station_name").alias("start_station_count"))
    )

    window_spec = Window.partitionBy("date").orderBy(
        F.col("start_station_count").desc()
    )

    ranked_df = count_df.withColumn("rank", F.row_number().over(window_spec))

    popular_station_df = ranked_df.filter(F.col("rank") <= 3).select(
        "date", "to_station_name", "start_station_count"
    )

    popular_station_df.coalesce(1).write.option("header", True).csv(
        f"{outputfilepath}/popular_stations_two_weeks"
    )


def calculateTripDurationByGender(df, outputfilepath: str):
    genderColumnExists = hasColumn(df, "gender")

    if genderColumnExists:
        gender_trips_avg = df.groupBy("gender").agg(
            F.avg("trip_duration").alias("avg_trip_duration")
        )

        gender_trips_avg.coalesce(1).write.option("header", True).csv(
            f"{outputfilepath}/gender_trips"
        )


def calculateTripDurationByAge(df, outputfilepath: str):
    birthYearColumnExists = hasColumn(df, "birthyear")

    if birthYearColumnExists:
        df = df.withColumn("age", getAgeFromYear(F.col("birthyear")))

        df = df.na.drop(subset=["age"])

        df = df.filter(F.col("trip_duration") >= 1)

        df = (
            df.groupBy("age")
            .agg(F.avg("trip_duration").alias("avg_trip_duration"))
            .orderBy(F.col("avg_trip_duration").desc())
        )

        longest_trip_df = df.limit(10)

        shortest_trip_df = df.orderBy(F.col("avg_trip_duration").asc()).limit(10)

        longest_trip_df.coalesce(1).write.option("header", True).csv(
            f"{outputfilepath}/longest_trip_duration_by_age"
        )

        shortest_trip_df.coalesce(1).write.option("header", True).csv(
            f"{outputfilepath}/shortest_trip_duration_by_age"
        )


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    files = getFiles("/data/*.zip")
    for file in files:
        csv_file_name = file.split("/")[-1].replace(".zip", "")

        with zipfile.ZipFile(file, "r") as f:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_path = temp_file.name
                f.extract(f"{csv_file_name}.csv", os.path.dirname(temp_path))

        df = (
            spark.read.format("csv")
            .option("header", True)
            .load(f"{os.path.dirname(temp_path)}/{csv_file_name}.csv")
        )

        df = df.withColumnRenamed("started_at", "start_time").withColumnRenamed(
            "ended_at", "end_time"
        )

        df = df.withColumn("start_time", F.col("start_time").cast("timestamp"))
        df = df.withColumn("end_time", F.col("end_time").cast("timestamp"))

        df = df.withColumn(
            "trip_duration",
            (F.col("end_time").cast("long") - F.col("start_time").cast("long")),
        )

        df = df.withColumn("date", F.to_date(F.col("start_time")))

        calculateAvgTripsPerDay(df, f"reports/{csv_file_name}")

        calcuateCountTripsPerDay(df, f"reports/{csv_file_name}")

        getPopularStartingStation(df, f"reports/{csv_file_name}")

        getLastTwoWeeksPopularTripStations(df, f"reports/{csv_file_name}")

        calculateTripDurationByGender(df, f"reports/{csv_file_name}")

        calculateTripDurationByAge(df, f"reports/{csv_file_name}")

    os.remove(temp_path)


if __name__ == "__main__":
    main()
