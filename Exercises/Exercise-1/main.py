import requests
import os
import zipfile
from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count
import time

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def createDownloadsFolder():
    if not os.path.exists("downloads"):
        os.makedirs("downloads")


def unzipFileToCSV(zipPath: str):
    zip_file = zipfile.ZipFile(zipPath, "r")
    for names in zip_file.namelist():
        zip_file.extract(names, os.path.join("downloads"))
    zip_file.close()


def deleteZipFile(zipPath: str):
    os.remove(zipPath)


def downloadFile(uri: str):
    # get filename from url
    uri_res_name = uri.split("/")[-1]

    # create path where file will be saved
    filepath = os.path.join("downloads", uri_res_name)
    timeNow = time.time()

    r = requests.get(uri, stream=True)
    if r.ok:
        print("saving to:", os.path.abspath(filepath))
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
    else:
        print("Download failed, status code {}\{}".format(r.status_code, r.text))
        return "Error downloading file"

    unzipFileToCSV(filepath)
    deleteZipFile(filepath)
    return f"Total Execution Time: {time.time() - timeNow} seconds"


def main():
    createDownloadsFolder()
    cpus = cpu_count()
    results = ThreadPool(cpus - 1).imap_unordered(downloadFile, download_uris)
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
