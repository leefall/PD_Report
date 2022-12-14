#!/usr/bin/env python

import os, glob
import os.path
import pandas as pd
import numpy as np
import gzip
import time
import os, subprocess, glob, time
import sys, time, random, re ,requests, logging, glob
import concurrent.futures
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager

logger=logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter=logging.Formatter("%(asctime)s - %(message)s")

sANNOVARDir="/storage/home/leefall2/clara/ANNOVAR/annovar"
sIntervarDir="/storage/home/leefall2/tools/Intervar/InterVar"
sVCFDir="/mnt/Beta/leefall2/PD/VCF/Filter"#
sOutputDir="/mnt/Beta/leefall2/PD/Intervar"

def ConvertInput(sFilename):
	sID=sFilename.split(".")[0]
	
	os.system(sANNOVARDir+"/convert2annovar.pl -format vcf4 "+sFilename+" > "+sID+".avinput")
	
def IntervarCall(sFilename):
	sID=sFilename.split(".")[0]
	
	os.system("python "+sIntervarDir+"/Intervar.py -b hg19 -i "+sID+".avinput -o "+sOutputDir+"/"+sID)



def producer_task(q, cosmic_dict):

	lVCFfiles=glob.glob("*.vcf")

		
	for sFilename in lVCFfiles:

		value=sFilename
	#logger.info("Producer [%s] putting value [%s] into queue.." % (current_process().name, value))
		q.put(value)




def consumer_task(q, cosmic_dict):
	while not q.empty():
		value=q.get(True, 0.05)
		
		
		ConvertInput(value)
		IntervarCall(value)
		
		
		
		

if __name__=="__main__":
	
	StartTime=(time.ctime())
	print(StartTime)
	data_queue=Queue()
	#os.chdir("/mnt/towel/leefall2/Gustaveraw/EGAF00002385295")
	os.chdir(sVCFDir)

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
#		
#		
