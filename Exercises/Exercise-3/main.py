import io
import requests
import gzip


def readAndExtractGzipFile(url):
    response = requests.get(url, stream=True)

    # Extract the contents of the gzipped file
    compressed_file = (response.raw)
    buffer = io.BytesIO()
    chunk_size = 1024

    while True:
        chunk = compressed_file.read(chunk_size)
        if not chunk:
            break
        buffer.write(chunk)

    buffer.seek(0)
    uncompressed_file = gzip.GzipFile(fileobj=buffer)

    decoded_contents = ""
    
    for line in uncompressed_file:
        decoded_line = line.decode("utf-8")
        decoded_contents += (decoded_line)
 
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
