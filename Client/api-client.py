import linecache
import json
import time
import requests
from pathlib import Path


folder = Path.cwd().parent / 'dataset' / 'Clean Data'
files = folder.glob("*.json")

# initialize a counter for successful responses
success_count = 0

# Loop over the JSON files
for file in files:
    # set endpoint based on file name
    if 'business' in file.name:
        endpoint = 'http://localhost:80/business'
    elif 'checkin' in file.name:
        endpoint = 'http://localhost:80/checkin'
    elif 'review' in file.name:
        endpoint = 'http://localhost:80/review'
    elif 'tip' in file.name:
        endpoint = 'http://localhost:80/tip'
    elif 'user' in file.name:
        endpoint = 'http://localhost:80/user'
    else:
        continue  # skip files that are not recognized

    # read the file and set starting and ending ids
    with open(file, "r") as f:
        end = sum(1 for line in f)

    start = 1

    # loop over the JSON objects in the file
    i = start
    while i <= end:
        # read a specific line
        line = linecache.getline(str(file), i)
        myjson = json.loads(line)

        # send the JSON object to the appropriate endpoint
        response = requests.post(endpoint, json=myjson)
        # time.sleep(0.5)

        # increment the counter if the response was successful
        if response.ok:
            success_count += 1

        # print the response (for debugging)
        print(response.json())

        # increase i
        i += 1

    # print a message when the file is done
    print(f"File {file.name} sent correctly")

# print the number of successful responses
print(f"Total number of successful responses: {success_count}")
