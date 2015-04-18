import sys,os
from random import randint

outputfile = ""

if __name__=="__main__":

	if len(sys.argv)==3:
		
		patterns1 = list();
		patterns2 = list();
		patterns3 = list();
		target = list();

		target_graph = sys.argv[1]
		k = int(sys.argv[2])

		filename1 = "f0_bf_k"+str(k)+".dat"
		filename2 = "f0_pca_k"+str(k)+".dat"
		filename3 = "f0_img_k"+str(k)+".dat"
		
		for line in open(filename1,"r"):
			patterns1.append(line.strip().split()[0]);

		for line in open(filename2,"r"):
			patterns2.append(line.strip().split()[0]);

		for line in open(filename3,"r"):
			patterns3.append(line.strip().split()[0]);


		for line in open(target_graph,"r"):
			targetline = list();
			for item in line.strip().split():
				targetline.append(item);
			target.append(targetline);


		ofilename = "result.dat"
		ofile  = open(ofilename,"w");

		index = 0;
		for i in range(0, 1):

			sum = 0.0;

			for line in patterns1:
				if(line==target[int(line)+1][0]):
					# print line,target[int(line)+1][12];
					sum = sum + float(target[int(line)+1][12])
				else:
					print "!!!Index error",line,target[int(line)+1][0]

			print "bfAME = ", 1.0/k*sum;


			sum = 0.0;

			for line in patterns2:
				if(line==target[int(line)+1][0]):
					# print line,target[int(line)+1][12];
					sum = sum + float(target[int(line)+1][12])
				else:
					print "!!!Index error",line,target[int(line)+1][0]

			print "pcaAME = ", 1.0/k*sum;


			sum = 0.0;

			for line in patterns3:
				if(line==target[int(line)+1][0]):
					# print line,target[int(line)+1][12];
					sum = sum + float(target[int(line)+1][12])
				else:
					print "!!!Index error",line,target[int(line)+1][0]

			print "imgAME = ", (sum*1.0/k);

			continue;

		print "=========================================="

	else:
		print "args: target_graph, top_k";



