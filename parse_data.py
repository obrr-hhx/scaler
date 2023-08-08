import json
import os
import sys
import math

data = {}

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
    end_time = {}
    idle_time = {}
    for d in data:
        if d["metaKey"] not in appCount:
            appCount[d["metaKey"]] = 0
            start_time[d["metaKey"]] = []
            start_time[d["metaKey"]].append(d["startTime"])
            end_time[d["metaKey"]] = []
            end_time[d["metaKey"]].append(d["startTime"] + d["durationsInMs"])
            idle_time[d["metaKey"]] = []
        else :
            appCount[d["metaKey"]] += 1
            start_time[d["metaKey"]].append(d["startTime"])
            end_time[d["metaKey"]].append(d["startTime"] + d["durationsInMs"])
    res = {}
    res["appCount"] = appCount
    for k, _ in start_time.items():
        start = start_time[k]
        end = end_time[k]
        for i in range(len(start)):
            idle = 100000000
            for j in range(len(end)):
                if start[i] - end[j] > 0 and start[i] - end[j] < idle:
                    idle = start[i] - end[j]
            if idle != 100000000:
                idle_time[k].append(idle)
    res["start_time"] = start_time
    res["end_time"] = end_time
    res["idle_time"] = idle_time
    return res

def read_metas(metasPath):
    global data
    data["metas"] = {}
    with open(metasPath, 'r') as f:
        # read file line by line
        for line in f:
            # convert line to json object
            d = json.loads(line)
            # res.append(d)
            data["metas"][d["key"]] = d["initDurationInMs"]

class Req:
    def __init__(self, r_type, r_start, r_duration):
        self.type_ = r_type
        self.startTime_ = r_start
        self.durationTime_ = r_duration
        self.initTime_ = 0
        self.endTime_ = 0
    def to_string(self) -> str:
        string = "request type: " + self.type_ + "\n"
        string += "\tstart time: " + str(self.startTime_) + "\n"
        if self.initTime_ != 0:
            string += "\tinit duration time: " + str(self.initTime_) +"ms\n"
        string += "\tduration time: " + str(self.durationTime_) + "ms\n"
        if self.endTime_ != 0:
            string += "\tend time: " + str(self.endTime_) + "\n"
        return string
    def addInitTime(self, init_time):
        self.initTime_ = init_time
    def endTime(self):
        self.endTime_ = self.startTime_ + self.initTime_ + self.durationTime_


def read_requests(requestPath):
    global data
    requests = {}
    with open(requestPath, 'r') as f:
        # read file line by line
        for line in f:
            # convert line to json object
            d = json.loads(line)
            if d["metaKey"] not in requests:
                requests[d["metaKey"]] = []

            request_type = d["metaKey"]
            startTime = d["startTime"]
            durationTime = d["durationsInMs"]
            request = Req(request_type, startTime, durationTime)
            requests[request_type].append(request)
                
    data["requests"] = requests

def parse_requests(requests):
    allRequests = []
    policy = {}
    for k, v in requests.items():
        policy[k] = {}
        policy[k]["pre_warm_window"] = 999999999999
        policy[k]["keep_alive_window"] = 0

        init_time = v[0].initTime_
        
        allRequests = v
    
        # sort all requests
        allRequests.sort(key=lambda x: x.startTime_)

        cluster = []
        idle_time = []
        j=0
        for i in range(1, len(allRequests)):
            if allRequests[i].startTime_ > allRequests[i-1].endTime_:
                cluster.append(i-j)
                idle_time.append(allRequests[i].startTime_ - allRequests[i-1].endTime_)
                j = i
        cluster.append(len(allRequests) - j)
        # print("requests come pattern: ", cluster)
        if len(idle_time) != 0:
            # print("idle time: ", idle_time)
            try:
                for it in idle_time:
                        if it > init_time and it <= policy[k]["pre_warm_window"]:
                            policy[k]["pre_warm_window"] = it
                # policy[k]["keep_alive_window"] = max(idle_time)
                idle_time.sort()
                policy[k]["keep_alive_window"] = idle_time[int(len(idle_time) * 95 / 100)]
            except:
                    print("Request type: ", k, "num:", len(v))
                    print(idle_time)
        if policy[k]["pre_warm_window"] == 999999999999:
            policy[k]["pre_warm_window"] = 0
    return policy

def parallel_requests(requests):
    create_slot = 200 # ms
    parallelism = {}
    for k, v in requests.items():
        parallelism[k] = {}
        parallel_cluster = []
        initTime = v[0].initTime_
        v.sort(key=lambda x: x.startTime_)
        i = 0
        while i < len(v)-1:
            for j in range(i+1, len(v)):
                if v[j].startTime_ - v[i].startTime_ > initTime + create_slot:
                    parallel_cluster.append(j - i)
                    i = j
                    break
            i += 1
        parallel_cluster.append(len(v) - i)

        max_parallel = max(parallel_cluster)
        if max_parallel < 0:
            print(parallel_cluster)
        max_i = parallel_cluster.index(max_parallel)
        parallel_strengthen = 0
        slot_num = 0
        for i in range(0, max_i):
            slot_num += parallel_cluster[i]
        if slot_num != 0:
            parallel_strengthen = math.ceil(max_parallel / slot_num)
        # print("application: ", k, "\nparallel cluster max: ", max_parallel, "slot num: ", slot_num)
        # print("parallel cluster strengthen: ")
        # print(parallel_strengthen, "\n")
        parallelism[k]["max_parallel_index"] = slot_num
        parallelism[k]["parallel_strengthen"] = parallel_strengthen
    return parallelism
                
if __name__ == '__main__':
    # check if the number of arguments is correct
    if len(sys.argv) != 2:
        print('Usage: python parse_data.py <input_directory>')
        sys.exit(1)
    # check if the input file exists
    if not os.path.exists(sys.argv[1]):
        print('Input directory does not exist!')
        sys.exit(1)
    
    # read data from input file
    metas_path = sys.argv[1] + "/metas"
    request_path = sys.argv[1] + "/requests"

    read_metas(metas_path)
    read_requests(request_path)

    # for k, v in data["metas"].items():
    #     print("request type: ", k, end=" ")
    #     print("init duration time: ", v, "ms")

    
    for k, v in data["requests"].items():
        initTime = data["metas"][k]
        for req in v:
            req.addInitTime(initTime)
    
    for _, v in data["requests"].items():
        for request in v:
            request.endTime()
            # print(request.to_string(), end="")

    policy = parse_requests(data["requests"])
    parallelism = parallel_requests(data["requests"])
    for k, _ in policy.items():
        policy[k]["max_parallel_index"] = parallelism[k]["max_parallel_index"]
        policy[k]["parallel_strengthen"] = parallelism[k]["parallel_strengthen"]

    json_name = sys.argv[1].split("/")[-1]
    json_name += ".json"
    print(json_name)
    with open(json_name, "w") as f:
        json.dump(policy, f, indent=4)