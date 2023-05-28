import io
import requests
import gzip


def readAndExtractGzipFile(url):
    response = requests.get(url)

    # Extract the contents of the gzipped file
    compressed_file = io.BytesIO(response.content)
    uncompressed_file = gzip.GzipFile(fileobj=compressed_file)

    file_contents = uncompressed_file.read()

    # Decode the file contents assuming it's UTF-8 encoded
    decoded_contents = file_contents.decode("utf-8")

    return decoded_contents

def getFileToPrintUrl(url):
    file_contents = readAndExtractGzipFile(url)

    first_file_url = file_contents.split("\n")[0]

    return first_file_url

def printFileContents(url):
    response = requests.get(url, stream=True)

    if response.ok:
        compressed_file = gzip.GzipFile(fileobj=response.raw)
        for line in compressed_file:
        # Process each line here
            decoded_line = line.decode("utf-8")
            print(decoded_line, end="")
    else:
        return
    

def main():
    original_url = "https://data.commoncrawl.org"
    file_to_get_url = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"
    first_file_url = getFileToPrintUrl(f"{original_url}/{file_to_get_url}")

    printFileContents(f"{original_url}/{first_file_url}")

if __name__ == "__main__":
    main()
