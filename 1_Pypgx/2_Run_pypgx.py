#!/usr/bin/env python

import gzip
import os, subprocess, glob, time
import sys, time, random, re ,requests, logging, glob
import concurrent.futures
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager



sVCFDir="/mnt/Beta/leefall2/PD/VCF/Filter"
sDepthDir="/mnt/Beta/leefall2/PD/Depth"
sStatisticsDir="/mnt/Beta/leefall2/PD/Control_Statistics"
sPypgxOutputDir="/mnt/Beta/leefall2/PD/pypgx"


logger=logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter=logging.Formatter("%(asctime)s - %(message)s")

ch=logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

#def ImportVCF(sGene,sFile):
	
	
	#pypgx import-variants IFNL3 /storage/home/leefall2/mypro/CKD/Parsed/Merged_VCF/CKD0017.vcf.gz --platform Targeted /storage/home/leefall2/mypro/CKD/Parsed/Merged_VCF/Test/pypgx/IFNL3_CKD0017




#def Phasing(sGene,sFile):
#	pypgx estimate-phase-beagle /storage/home/leefall2/mypro/CKD/Parsed/Merged_VCF/Test/pypgx/IFNL3_CKD0017 /storage/home/leefall2/mypro/CKD/Parsed/Merged_VCF/Test/pypgx/Phased_IFNL3_CKD0017


def RunPGx(sFile):
	
	lTargetgenes=['cyp2c19', 'cyp2d6', 'cyp2e1', 'cyp2f1', 'cyp2j2', 'cyp2r1', 'cyp2s1', 'cyp2w1', 'cyp3a4', 'cyp3a5', 'cyp3a7', 'cyp3a43', 'cyp4b1', 'cyp4f2', 'cyp19a1', 'cyp26a1', 'dpyd', 'g6pd', 'gstm1', 'gstp1', 'gstt1', 'ifnl3', 'nat1', 'nat2', 'nudt15', 'por', 'ryr1', 'slc15a2', 'slc22a2', 'slco1b1', 'slco1b3', 'slco2b1', 'sult1a1', 'tbxas1', 'tpmt', 'ugt1a1', 'ugt1a4', 'ugt2b7', 'ugt2b15', 'ugt2b17']
	sID=sFile.split(".")[0]
	for sGene in lTargetgenes:
		sGene=sGene.upper()
		print(sGene)
		os.system('''
	
pypgx run-ngs-pipeline \
'''+sGene+''' \
'''+sPypgxOutputDir+'''/'''+sID+'''/'''+sGene+''' \
--variants '''+sVCFDir+'''/'''+sFile+''' \
--depth-of-coverage '''+sDepthDir+'''/'''+sID+'''.zip \
--control-statistics '''+sStatisticsDir+'''/'''+sID+'''.zip \
--force \
--do-not-plot-allele-fraction \
--do-not-plot-copy-number 

''')




def producer_task(q, cosmic_dict):


	lVCFlist=glob.glob("*.vcf.gz")
for sFilename in lVCFlist:

		value=sFilename

		q.put(value)

def consumer_task(q, cosmic_dict):
	while not q.empty():
		value=q.get(True, 0.05)

#		PredictAllele(value)
		RunPGx(value)


		

if __name__=="__main__":
	StartTime=(time.ctime())
	data_queue=Queue()
	
	os.chdir(sVCFDir)
#	number_of_cpus=cpu_count()-2
	number_of_cpus=8
	manager=Manager()
	fibo_dict=manager.dict()
	producer=Process(target=producer_task, args=(data_queue, fibo_dict))
	producer.start()
	producer.join()
	consumer_list=[]
	#print(fibo_dict)
	for i in range(number_of_cpus):
		consumer=Process(target=consumer_task, args=(data_queue,fibo_dict))
		consumer.start()
		consumer_list.append(consumer)

	[consumer.join() for consumer in consumer_list]





	print("Start Time")
	print(StartTime)
	print("End Time")
	print(time.ctime())