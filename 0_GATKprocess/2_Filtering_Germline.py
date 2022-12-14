#!/usr/bin/env python
import gzip
import numpy as np
import os, subprocess, glob, time, tabix
import sys, time, random, re ,requests, logging, glob
import concurrent.futures
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager

sVCFDir="/mnt/Beta/leefall2/PD/VCF"
logger=logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter=logging.Formatter("%(asctime)s - %(message)s")

ch=logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)



def GATKVCFFilter(sFile):
	
	try:
		os.mkdir("./Temporaly")
	except:
		pass
	
	try:
		os.mkdir("./Filter")
	except:
		pass
	
	
	
	subprocess.call('''

/mnt/QNAP/leefall2/gatk-4.1.0.0/gatk --java-options "-Xmx8G" SelectVariants \
--tmp-dir=/mnt/QNAP/leefall2/gatk-4.1.0.0/tmp \
-V '''+sFile+''' \
-select-type SNP \
-O ./Temporaly/SNV_'''+sFile+''' 


/mnt/QNAP/leefall2/gatk-4.1.0.0/gatk --java-options "-Xmx8G" SelectVariants \
--tmp-dir=/mnt/QNAP/leefall2/gatk-4.1.0.0/tmp \
-V '''+sFile+''' \
-select-type INDEL \
-O ./Temporaly/INDEL_'''+sFile+''' 


/mnt/QNAP/leefall2/gatk-4.1.0.0/gatk --java-options "-Xmx8G" VariantFiltration \
--tmp-dir=/mnt/QNAP/leefall2/gatk-4.1.0.0/tmp \
-V ./Temporaly/SNV_'''+sFile+''' \
-filter "QD < 2.0" --filter-name "QD2" \
-filter "DP < 10.0" --filter-name "LowDP10" \
-filter "QUAL < 30.0" --filter-name "QUAL30" \
-filter "SOR > 3.0" --filter-name "SOR3" \
-filter "FS > 60.0" --filter-name "FS60" \
-filter "MQ < 40.0" --filter-name "MQ40" \
-filter "MQRankSum < -12.5" --filter-name "MQRankSum-12.5" \
-filter "ReadPosRankSum < -8.0" --filter-name "ReadPosRankSum-8" \
--cluster-size 3 \
--cluster-window-size 50 \
-O ./Temporaly/Filtered_SNV_'''+sFile+'''


/mnt/QNAP/leefall2/gatk-4.1.0.0/gatk --java-options "-Xmx8G" VariantFiltration \
--tmp-dir=/mnt/QNAP/leefall2/gatk-4.1.0.0/tmp \
-V ./Temporaly/INDEL_'''+sFile+''' \
-filter "QD < 2.0" --filter-name "QD2" \
-filter "DP < 10.0" --filter-name "LowDP10" \
-filter "QUAL < 30.0" --filter-name "QUAL30" \
-filter "FS > 200.0" --filter-name "FS200" \
-filter "ReadPosRankSum < -20.0" --filter-name "ReadPosRankSum-20" \
--cluster-size 3 \
--cluster-window-size 50 \
-O ./Temporaly/Filtered_INDEL_'''+sFile+'''






#Remove temporal data


''',shell=True)
	fout=open("./Temporaly/Unsorted_"+sFile.replace(".gz",""),"w")
	fSNV=gzip.open('''./Temporaly/Filtered_SNV_'''+sFile,"rt")
	
	for sLine in fSNV.readlines():
		if sLine[0]=="#":
			fout.write(sLine)
		else:
			t=sLine.split("\t")
			if t[6]=="PASS":
				fout.write(sLine)
	fSNV.close()
	
	fINDEL=gzip.open('''./Temporaly/Filtered_INDEL_'''+sFile,"rt")
	
	for sLine in fINDEL.readlines():
		if sLine[0]=="#":
			pass
		else:
			t=sLine.split("\t")
			if t[6]=="PASS":
				fout.write(sLine)
			
	
	fout.close()
	os.system("vcf-sort ./Temporaly/Unsorted_"+sFile.replace(".gz","")+" >./Filter/"+sFile.replace(".gz",""))
	os.system("bgzip ./Filter/"+sFile.replace(".gz",""))
	#Remove Temporaly files
	
	
def RemoveTemporal(sFile):
	
	os.system('''rm ./Temporaly/SNV_'''+sFile)
	os.system('''rm ./Temporaly/INDEL_'''+sFile)
	os.system('''rm ./Temporaly/Filtered_SNV_'''+sFile)
	os.system('''rm ./Temporaly/Filtered_INDEL_'''+sFile)
	os.system("rm ./Temporaly/Unsorted_"+sFile.replace(".gz",""))
	
	

def IndexVCF(sFile):
	
	os.system("tabix -p vcf ./Filter/"+sFile)
	
	
	


def VCF_to_Variant(sFile):

	GATKVCFFilter(sFile)
	RemoveTemporal(sFile)
	IndexVCF(sFile)
	





def consumer_task(q, cosmic_dict):
	dDeleteriousDict=dict()
	while not q.empty():
		value=q.get(True, 0.05)
		VCF_to_Variant(value)





		cosmic_dict[value]="complete"
		logger.info("consumer [%s] getting value [%s] from queue..." % (current_process().name, value))


def producer_task(q, cosmic_dict):

	sFilelist=glob.glob("*.vcf.gz")
	#sFilelist=glob.glob("TCGA-YZ-A985-10*.vcf.gz")
	sFilelist=sFilelist
	for i in sFilelist:
		value=i
		cosmic_dict[value]=None
		logger.info("Producer [%s] putting value [%s] into queue.." % (current_process().name, value))
		q.put(value)
#		else:
			#pass





if __name__=="__main__":
	StartTime=(time.ctime())
	data_queue=Queue()

	number_of_cpus=6


	
	os.chdir(sVCFDir)
#	try:
#		os.mkdir("Filtering")
#	except:
#		pass


	manager=Manager()
	fibo_dict=manager.dict()
	producer=Process(target=producer_task, args=(data_queue, fibo_dict))
	producer.start()
	producer.join()
	consumer_list=[]
	for i in range(number_of_cpus):
		consumer=Process(target=consumer_task, args=(data_queue,fibo_dict))
		consumer.start()
		consumer_list.append(consumer)

	[consumer.join() for consumer in consumer_list]

	logger.info(fibo_dict)
