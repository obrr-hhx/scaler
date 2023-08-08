import os
import sys

def add_colon(file_name):
    res = []
    with open(file_name, 'r') as f:
        # read file line by line
        for line in f:
            # if the last character is not a comma, add a comma
            if line == '\n':
                continue
            if line[-2] != ',' and line[-2] != '{':
                line = line[:-1] + ',\n'
            res.append(line)
    file_name.replace('.go', '_comma.go')
    with open(file_name, 'w') as f:
        for line in res:
            f.write(line)

if __name__ == '__main__':
    add_colon(sys.argv[1])