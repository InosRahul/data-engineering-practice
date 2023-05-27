import requests
import pandas
from bs4 import BeautifulSoup
import os

def getTargetFileUrl(uri:str, lastModified: str):
    response = requests.get(uri)

    soup = BeautifulSoup(response.content, 'html.parser')

    # the links of the table rows 
    links = soup.find_all('a')[5:]

    # Iterate over the links and find the file with the specified last modified date
    target_file_url = None
    for link in links:
        href = link.get('href')
        # need to check for file for a specific datetime
        file_datetime = link.parent.next_sibling.get_text(strip=True)
        if file_datetime == lastModified:
            target_file_url = href
            break
    return uri + target_file_url

def downloadTargetFile(url: str):
    response = requests.get(url)
    filename = url.split("/")[-1]
    if response.ok:
        print("saving to", os.path)
        open(filename, "wb").write(response.content)
    return filename

def calculateHighestHourlyDryBulbTemperature(df: pandas.DataFrame):
        return df['HourlyDryBulbTemperature'].max()


def main():
    targetFileUrl = getTargetFileUrl("https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/","2022-02-07 14:03" )
    downloadedFilename = downloadTargetFile(targetFileUrl)

    df = pandas.read_csv(downloadedFilename)

    maxTemp = calculateHighestHourlyDryBulbTemperature(df)

    print(maxTemp)

if __name__ == "__main__":
    main()
