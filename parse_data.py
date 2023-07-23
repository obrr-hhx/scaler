import json
import os
import sys

# read data from given file
def read_data(file_name):
    res = []
    with open(file_name, 'r') as f:
        # read file line by line
        for line in f:
            # convert line to json object
            data = json.loads(line)
            res.append(data)
    return res

def parse_data(data):
    appCount = {}
    start_time = {}
    start_time_interval = {}
    for d in data:
        if d["metaKey"] not in appCount:
            appCount[d["metaKey"]] = 0
            start_time[d["metaKey"]] = d["startTime"]
            start_time_interval[d["metaKey"]] = []
        else :
            appCount[d["metaKey"]] += 1
            pre_start_time = start_time[d["metaKey"]]
            now_start_time = d["startTime"]
            start_time[d["metaKey"]] = now_start_time
            start_time_interval[d["metaKey"]].append(now_start_time - pre_start_time)
    res = {}
    res["appCount"] = appCount
    interval = {}
    for key in start_time_interval:
        interval[key] = sum(start_time_interval[key]) / len(start_time_interval[key])
    res["start_time_interval"] = interval
    return res

if __name__ == '__main__':
    # check if the number of arguments is correct
    if len(sys.argv) != 2:
        print('Usage: python parse_data.py <input_file>')
        sys.exit(1)
    # check if the input file exists
    if not os.path.exists(sys.argv[1]):
        print('Input file does not exist!')
        sys.exit(1)

    # read data from input file
    data = read_data(sys.argv[1])
    # parse data
    res = parse_data(data)
    # print result
    print(res)