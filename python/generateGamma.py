import sys,os
from random import randint

outputfile = ""

def dt(d):
	return -d['c']

if __name__=="__main__":

	if len(sys.argv)==3:
		
		olist = list();

		filename = sys.argv[1]
		plabel = sys.argv[2]
		
		for line in open(filename,"r"):
			olist.append(line.strip());

		ofilename = filename[:filename.rfind("/")+1]+"pr-"+filename[filename.rfind("/")+1:filename.rfind(".")]+"-"+plabel+".gamma"
		ofile  = open(ofilename,"w");

		index = 0;
		for i in range(0, len(olist)):
			for j in range(0, len(olist)):
				if i == j:
					continue;
				else:
					ofile.write("==============id="+str(index)+"============\n");
					ofile.write("0\t1\n");
					ofile.write("0\t1\t1\t2\t3\t4\n");
					ofile.write("1\t"+plabel+"\n");
					ofile.write("2\t1\t4\n");
					ofile.write("3\t1\t5\n");
					ofile.write("4\t"+olist[i]+"\n");
					ofile.write("5\t"+olist[j]+"\n");
					index = index +1;
					ofile.write("==============id="+str(index)+"============\n");
					ofile.write("0\t1\n");
					ofile.write("0\t1\t1\t2\t3\t4\n");
					ofile.write("1\t"+plabel+"\n");
					ofile.write("2\t1\t5\n");
					ofile.write("3\t1\t5\n");
					ofile.write("4\t"+olist[i]+"\n");
					ofile.write("5\t"+olist[j]+"\n");
					index = index +1;
		ofile.write("===================================");
		ofile.close();

	else:
		print "args: freq-edge-set-file, plabel";



