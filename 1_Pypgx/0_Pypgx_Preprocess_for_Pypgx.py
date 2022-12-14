#!/usr/bin/env python
import gzip
import os, subprocess, glob, time
import sys, time, random, re ,requests, logging, glob
import concurrent.futures
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager


logger=logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter=logging.Formatter("%(asctime)s - %(message)s")

ch=logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)



dPDFileDict=dict()
fp=open("/mnt/Beta/leefall2/PD/Code/PD_path.txt")

fp.readline()
for sLine in fp.readlines():
	sLine=sLine.strip()
	t=sLine.split("\t")
	(sID,sBam,sVCF)=(t[0],t[3],t[4])
	
	
	dPDFileDict[sID]=[sBam,sVCF]
	
	
	

#java -Xmx10G -Djava.io.tmpdir=$TMP \
#-jar $PICARD/ReorderSam.jar \
#INPUT=$BAM/$input".bam" \
#OUTPUT=$Process/$input"_sorted.bam" \
#REFERENCE=$REF/ucsc.hg19.fasta \
#ALLOW_CONTIG_LENGTH_DISCORDANCE=TRUE



def ControlStatistics(sID):
	
	sBamFile=dPDFileDict[sID][0]
	
	subprocess.call("pypgx compute-control-statistics VDR /mnt/Beta/leefall2/PD/Control_Statistics/"+sID+".zip "+sBamFile,shell=True)
	
	
	
	
def Depth(sID):
	sBamFile=dPDFileDict[sID][0]
	subprocess.call('''
pypgx prepare-depth-of-coverage /mnt/Beta/leefall2/PD/Depth/'''+sID+'''.zip '''+sBamFile+'''

''',shell=True)


def Preprocess(sID):
	ControlStatistics(sID)
	Depth(sID)
	#BamSort(sFile)
	#RemoveSam(sFile)
	#RemoveBam(sFile)





if __name__=="__main__":
	StartTime=(time.ctime())
	data_queue=Queue()
	#os.chdir("/mnt/Beta/CKD/HN00161065")
#	number_of_cpus=cpu_count()-2
	number_of_cpus=6
	
	
	manager=Manager()

	p = Pool(number_of_cpus)
	#sFilelist=glob.glob("/mnt/towel/UKBiobank/FE_vcfs/*.gvcf.gz")#4011848
	lFilelist=list(dPDFileDict.keys())
	#lFilelist=lFilelist[0:1]
	Pool_map = p.map(Preprocess, lFilelist)
	
	print("Start Time")
	print(StartTime)
	print("End Time")
	print(time.ctime())
