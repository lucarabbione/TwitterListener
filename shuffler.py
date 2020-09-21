#!/usr/bin/env python
import sys
import os

if __name__ == '__main__':
    lines = [] 
    for filename in os.listdir(sys.argv[1]):
       inputfile = open(os.path.join(sys.argv[1], filename), "r", encoding = 'utf-8', errors='ignore')
       lines.extend(inputfile.readlines())
       inputfile.close()
    lines = sorted(lines)
    outputfile = open(sys.argv[2], "w", encoding = 'utf-8', errors='ignore')
    outputfile.writelines(lines)
    outputfile.close()
