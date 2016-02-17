import numpy

lengths = {}
with open('train_timestamps.csv') as fp:
	for l in fp:
		p = l.strip().split(',')
		n = len(p)-1
		if n in lengths:
			lengths[n] += 1
		else:
			lengths[n] = 1
		
		
lenarr = []
for k,v in lengths.items():
	row = [k,v]
	lenarr.append(row)
	
	
lenarr = numpy.array(lenarr)
sorted_lenarr = lenarr[lenarr[:, 1].argsort()]

with open('train_sorted_numaccess.csv','w') as ofp:
	for row in sorted_lenarr:
		ofp.write(','.join([str(row[0]),str(row[1])])+'\n')
		
	
'''
regression:
	n timestamps
	difference in time between consecutive timestamps
	window of 10 time differences, predict the 11th time difference (we can classify this into the 4 classes depending on a rule that we have)
	move this window forward.
	


classification:
n timestamps:
	nth one is for prediction
	n-1 are bucketted into following:
		last day
		last week
		last month
'''