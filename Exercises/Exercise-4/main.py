import glob
import os
import json
import csv

def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out

def getFiles(pathStr:str):
    files = glob.glob(os.getcwd() + pathStr, recursive=True)
    return files

def convertJsonToCsv(files):
    for file in files:
        with open(file, 'rb') as jsonFile:
            json_file = json.load(jsonFile)
            json_flat = flatten_json(json_file)

        file = file.replace('.json', '')

        with open(f'{file}.csv', 'w+') as file:
            writer = csv.writer(file)
            writer.writerow(json_flat.keys())
            writer.writerow(json_flat.values())

def main():
    files = getFiles("/data/**/*.json")
    convertJsonToCsv(files)

if __name__ == "__main__":
    main()
