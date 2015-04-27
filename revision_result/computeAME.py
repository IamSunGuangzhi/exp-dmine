import sys,os
from random import randint

outputfile = ""

if __name__=="__main__":

	if len(sys.argv)==3:
		
		set1 = set();
		set2 = set();
		set3 = set();
		set4 = set();
		# target = list();

		target_graph = sys.argv[1]
		k = int(sys.argv[2])

		filename1 = "f0_pca_k"+str(k)+".dat"
		filename2 = "f0_bf_k"+str(k)+".dat"
		filename3 = "f0_bfi_k"+str(k)+".dat"
		filename4 = "f0_img_k"+str(k)+".dat"
		
		for line in open(filename1,"r"):
			set1.add(line.strip().split()[0]);

		for line in open(filename2,"r"):
			set2.add(line.strip().split()[0]);

		for line in open(filename3,"r"):
			set3.add(line.strip().split()[0]);

		for line in open(filename4,"r"):
			set4.add(line.strip().split()[0]);

		print "set size: ", len(set1), len(set2), len(set3), len(set4);

		pcaAME = 0.0;
		bfAME = 0.0;
		bfiAME = 0.0;
		imgAME = 0.0;

		count1 = 0;
		count2 = 0;
		count3 = 0;
		count4 = 0;

		for line in open(target_graph,"r"):
			target = line.strip().split();
			if target[0] in set1:
				pcaAME = pcaAME + float(target[13]);
				count1 = count1+1;
			if target[0] in set2:
				bfAME = bfAME + float(target[13]);
				count2 = count2+1;
			if target[0] in set3:
				bfiAME = bfiAME + float(target[13]);
				count3 = count3 +1;
			if target[0] in set4:
				imgAME = imgAME + float(target[13]);
				count4 = count4 +1;


		print "count ", count1, count2, count3, count4;
		print "pcaAME", pcaAME/k;
		print "bfAME", bfAME/k;
		print "bfiAME", bfiAME/k;
		print "imgAME", imgAME/k;

	else:
		print "args: target_graph, top_k";



