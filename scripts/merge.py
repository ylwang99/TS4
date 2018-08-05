#!/usr/bin/env python
import sys

if len(sys.argv) < 3:
	print "Arg: model name, input path, output path"

file_to = open(sys.argv[3], "a")
file_to.write(sys.argv[1] + "\n")
file = open(sys.argv[2], "r")
lines = file.readlines()
for line in lines:
	tokens = line.strip().split()
	if len(tokens) <= 3:
		file_to.write(line)
file.close()
file_to.close()

