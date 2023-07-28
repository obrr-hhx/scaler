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
    duration_time = {}
    for key in start_time_interval:
        try:
            interval[key] = sum(start_time_interval[key]) / len(start_time_interval[key])
            duration_time[key] = sum(start_time_interval[key])
        except ZeroDivisionError:
            continue
    res["start_time_interval"] = interval
    res["duration_time"] = duration_time
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
    print("app Number: ", len(res["appCount"]))
    for key, value in res["appCount"].items():
        if value > 500:
            print("high frequency request: ", key, " ", value)
            print("average start time interval: ", res["start_time_interval"][key])
            print("app duration time: ", res["duration_time"][key]/1000, "s")
    # print(res)