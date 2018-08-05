# Average trials of files
# Arg: file1, file2, file3... outputfile
# Output: 
# model
# clusterNum tiral1 trial2 trial3 ... average confidenceinterval paired_t_test:down/up

#!/usr/bin/env python
import sys
import math

fileNum = len(sys.argv) - 1
file_to = open(sys.argv[fileNum], "a")
clusterNum = 100
dt = {}
model = ""
index = 0
maximum = 0
for i in range(fileNum - 1):
	file = open(sys.argv[i + 1], "r")
	lines = file.readlines()
	for line in lines:
		tokens = line.strip().split()
		if len(tokens) == 1:
			model = tokens[0]
			if model not in dt:
				dt[model] = []
				for j in range(clusterNum):
					dt[model].append([])
			index = 0
		else:
			dt[model][index].append(float(tokens[len(tokens) - 1]))
			maximum = float(tokens[len(tokens) - 1])
			index = index + 1
	file.close()

for m in dt.keys():
	file_to.write(m + "\n")
	for i in range(clusterNum):
		file_to.write(str(i + 1) + "\t")
		sum = 0.0
		for value in dt[m][i]:
			file_to.write(str(value) + "\t")
			sum = sum + value
		mean = sum / len(dt[m][i])
		file_to.write(str(mean) + "\t")
		deviation = 0.0
		for value in dt[m][i]:
			deviation = deviation + (value - mean) * (value - mean)
		deviation = math.sqrt(deviation)
		interval = 1.96 * deviation / math.sqrt(len(dt[m][i]))
		file_to.write(str(interval) + "\t")
		if mean - interval > maximum:
			file_to.write("up\n")
		else:
			if mean + interval < maximum:
				file_to.write("down\n")
			else:
				file_to.write("none\n")

file_to.close()

