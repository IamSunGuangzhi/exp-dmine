import sys,os
from random import randint

outputfile = ""

if __name__=="__main__":

	if len(sys.argv)==3:
		
		topk_ptns = list();
		target_ptns = list();
		topk_ptns_ID = set();
		# target = list();

		target_graph = sys.argv[1]
		topk_file = sys.argv[2]

		print "target file:", target_graph;
		print "top_k_file", topk_file

		
		for line in open(topk_file,"r"):
			pattern = line.strip().split();
			topk_ptns.append(pattern);
			topk_ptns_ID.add(pattern[0]);

		print "set size = ",len(topk_ptns_ID)

		for line in open(target_graph,"r"):
			target_ptn = line.strip().split();
			if target_ptn[0] in topk_ptns_ID:
				target_ptns.append(target_ptn)

		print "target patterns", len(target_ptns)

		total_match = 0
		total_match_image = 0
		for item in target_ptns:
			total_match = total_match + int(item[2])
			total_match_image = total_match_image + int(item[3])
			print item;

		print "total_match  = ", total_match;
		print "total_match_image = ", total_match_image;

		pcaAME = 0.0;
		bfAME = 0.0;
		bfiAME = 0.0;

		for item in target_ptns:
			# print (float(item[2])/total_match),item[9],item[13],abs(float(item[9])-float(item[13])),(float(item[2])/total_match)*abs(float(item[9])-float(item[13]))
			pcaAME = pcaAME + (float(item[2])/total_match)*abs(float(item[9])-float(item[13]));
			bfAME = bfAME + (float(item[2])/total_match)*abs(float(item[10])-float(item[13]));
			# bfiAME = bfiAME + (float(item[3])/total_match_image)*abs(float(item[11])-float(item[13]));
			bfiAME = bfiAME + (float(item[3])/total_match)*abs(float(item[11])-float(item[13]));

		print "pcaAME = ",pcaAME;
		print "bfAME = ",bfAME;
		print "bfiAME = ",bfiAME;


		ofilename = "result.dat"
		ofile  = open(ofilename,"w");

		index = 0;
		# for i in range(0, 1):

		# 	sum = 0.0;

		# 	for line in patterns1:
		# 		if(line==target[int(line)+1][0]):
		# 			# print line,target[int(line)+1][12];
		# 			sum = sum + float(target[int(line)+1][12])
		# 		else:
		# 			print "!!!Index error",line,target[int(line)+1][0]

		# 	print "bfAME = ", 1.0/k*sum;


		# 	continue;

		print "=========================================="

	else:
		print "args: target_graph, top_k_file";



