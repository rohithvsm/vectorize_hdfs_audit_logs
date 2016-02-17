'''This method converts the successive timestamps into
difference in consecutive timestamps.

created: 7th December, 2015
author: Rohith Subramanyam <rohithvsm@gmail.com>
'''

# required packages and modules
import os
import sys
from datetime import datetime

def do(file1, file2):
	with open(file1) as fp, open(file2,'w') as ofp:
		for line in fp:
			parts = line.strip().split(',')
			if len(parts) < 20:
				continue
			name = parts[0]
			if len(parts[1:]) == 1:
				continue
			timestamps = []
			for t in parts[1:]:
				temp = t+"000"
				temp = datetime.strptime(temp, '%Y-%m-%d %H:%M:%S %f')
				timestamps.append(temp)
			# now work on the timestamps variable to find the difference
			# between two consecutive timestamps
			diff = []
			for i in range(len(timestamps)-1):
				timediff = (timestamps[i+1]-timestamps[i]).total_seconds()
				print(timediff)
				diff.append(str(timediff))
			ofp.write(','.join([name]+diff)+'\n')


def main():
	args = sys.argv

	if len(args) > 1:
		file1 = args[1]
		file2 = args[2]
	else:
		file1 = "../data/train_timestamps_1.csv"
		file2 = "../data/train_timediff.csv"

	do(file1, file2)

if __name__ == '__main__':
	main()
