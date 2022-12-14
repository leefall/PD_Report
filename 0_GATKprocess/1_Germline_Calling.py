#!/usr/bin/env python
import gzip
import os, subprocess, glob, time
import sys, time, random, re ,requests, logging, glob
import concurrent.futures
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager


sInputDir="/mnt/Beta/leefall2/PD/FinalBam"
sOutputDir="/mnt/Beta/leefall2/PD/VCF"
try:
	os.mkdir(sOutputDir)
except:
	pass

logger=logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter=logging.Formatter("%(asctime)s - %(message)s")

ch=logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)


def GATKHaplotypecaller(sFile):
	#sFile=dSampleDict[sTCGANormalID]
	sID=sFile.split(".")[0]

	subprocess.call('''

/mnt/QNAP/leefall2/gatk-4.1.4.1/gatk --java-options "-Xmx8G" HaplotypeCaller \
--tmp-dir=/mnt/QNAP/leefall2/gatk-4.1.0.0/tmp \
-R /mnt/QNAP/reference/GATK/hg19/ucsc.hg19.fasta \
-I '''+str(sFile)+''' \
-O '''+sOutputDir+'''/'''+str(sID)+'''.vcf.gz 




#Remove temporal data


''',shell=True)


def producer_task(q, cosmic_dict):

	
	lBamlist=glob.glob("*.bam")
	
	
	
	

	sFilelist=sFilelist
	#sFilelist=["IonXpress_001_R_2017_07_05_12_17_40_user_S5XL-0059-39-20170704_KPCDx_Test_Auto_user_S5XL-0059-39-20170704_KPCDx_Test_103.bam"]
	for i in sFilelist:
		#value=random.randint(1,len(sFilelist))
		#sCosmicFile=glob.glob("Cosmic_"+i)
		#if not sCosmicFile:
		value=i
		cosmic_dict[value]=None

		#logger.info("Producer [%s] putting value [%s] into queue.." % (current_process().name, value))
		q.put(value)
#		else:
			#pass


def consumer_task(q, cosmic_dict):
	while not q.empty():
		value=q.get(True, 0.05)

		GATKHaplotypecaller(value)

		cosmic_dict[value]="complete"
		logger.info("consumer [%s] getting value [%s] from queue..." % (current_process().name, value))





if __name__=="__main__":
	StartTime=(time.ctime())
	data_queue=Queue()
	os.chdir(sInputDir)
#	number_of_cpus=cpu_count()-2
	number_of_cpus=6
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





#	os.system("cp /home/Pathology/Ion_Torrent/Cancer_Panel/code/ANNOVAR_annotation_cancer_Panel.py .")
#	os.system("python ANNOVAR_annotation_cancer_Panel.py")








	print("Start Time")
	print(StartTime)
	print("End Time")
	print(time.ctime())
