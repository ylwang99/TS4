import sys
import os

file_vector = open("../data/wordvectors.txt", "r")

d = 50
dt_vector = {}
cnt = 0
lines = file_vector.readlines()
for line in lines:
	line = line.strip().split()
	dt_vector[line[0]] = []
	for i in range(d):
		dt_vector[line[0]].append(float(line[i+1]))
	cnt = cnt + 1
	if cnt % 100000 == 0:
		print str(cnt) + " words processed"
file_vector.close()
print str(cnt) + " total words"
print "Done recording word vectors"

file = open("../data/querytext2011.txt", "r")
path_to_mean = "../data/queryvec2011_mean.txt"
if os.path.exists(path_to_mean):
	os.remove(path_to_mean)
file_to_mean = open(path_to_mean, "a")
path_to_max = "../data/queryvec2011_max.txt"
if os.path.exists(path_to_max):
        os.remove(path_to_max)
file_to_max = open(path_to_max, "a")
lines = file.readlines()
cnt = 0
for line in lines:
	line = line.strip().split()
	if len(line) > 0:
		sum = [0]*d
		max = [-sys.maxint - 1]*d
		for word in line:
			for i in range(d):
				sum[i] = sum[i] + dt_vector[word][i]
				if dt_vector[word][i] > max[i]:
					max[i] = dt_vector[word][i] 
		for i in range(d):
			file_to_mean.write(str(sum[i] / len(line)) + " ")
			file_to_max.write(str(max[i]) + " ")
		file_to_mean.write("\n")
		file_to_max.write("\n")
		cnt = cnt + 1
print "Done"
file_to_mean.close()
file_to_max.close()
file.close()
