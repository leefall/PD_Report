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

sTemporDir="/mnt/Beta/leefall2/PD/Preprocess"
sOutputDir="/mnt/Beta/leefall2/PD/FinalBam"
lOutput=[]

lOutputFiles=glob.glob(sOutputDir+"/*.bam")
for sFile in lOutputFiles:
	sID=sFile.split("/")[-1]
	sID=sID.split(".")[0]
	lOutput.append(sID)




dPDFileDict=dict()
fp=open("PD_path.txt")

fp.readline()
for sLine in fp.readlines():
	sLine=sLine.strip()
	t=sLine.split("\t")
	(sID,sFast1,sFast2)=(t[0],t[1],t[2])
	
	
	dPDFileDict[sID]=[sFast1,sFast2]
	
	



def bwa(sID):

#		pass
	sFile=dPDFileDict[sID][0]

	sPartner=dPDFileDict[sID][1]
	subprocess.call('''
	




bwa-mem2 mem -t 12 -R "@RG\\tID:'''+sID+'''\\tPL:illumina\\tLB:WGS\\tSM:'''+sID+'''" /mnt/QNAP/reference/GATK/hg19/ucsc.hg19.fasta '''+sFile+''' '''+sPartner+''' > '''+sTemporDir+'''/'''+sID+'''.sam



''',shell=True)


def Bam_Sort(sID):
	
	

	
	
	subprocess.call('''
samtools view -bhS '''+sTemporDir+'''/'''+sID+'''.sam > '''+sTemporDir+'''/'''+sID+'''.bam
samtools sort '''+sTemporDir+'''/'''+sID+'''.bam -o '''+sTemporDir+'''/sorted_'''+sID+'''.bam

''',shell=True)



def MarkDuplicate(sID):




	subprocess.call('''

java -Xmx8G -Djava.io.tmpdir=/storage/home/leefall2/mypro/Cancer_Panel_Package/Package/temporary -jar /storage/home/leefall2/mypro/Cancer_Panel_Package/Package/Tools/picard_tools_1.108/MarkDuplicates.jar \
AS=TRUE \
I='''+sTemporDir+'''/sorted_'''+sID+'''.bam \
O='''+sTemporDir+'''/dedup_sorted_'''+sID+'''.bam  \
METRICS_FILE='''+sTemporDir+'''/'''+sID+'''_duplicates \
REMOVE_DUPLICATES=true \
CREATE_INDEX=True



''',shell=True)

def BaseRecalibrate(sID):

	subprocess.call('''


### Base Quality Score Recalibration
##  1) Base Recalibrator


/mnt/QNAP/leefall2/gatk-4.1.0.0/gatk --java-options "-Xmx8G" BaseRecalibrator \
--tmp-dir=/mnt/QNAP/leefall2/gatk-4.1.0.0/tmp \
-I '''+sTemporDir+'''/dedup_sorted_'''+sID+'''.bam \
-R /mnt/QNAP/reference/GATK/hg19/ucsc.hg19.fasta \
--known-sites /mnt/QNAP/reference/GATK/hg19/dbsnp_138.hg19.vcf \
-O '''+sTemporDir+'''/recal_'''+str(sID)+'''.table

## 2) Apply Recalibrate

/mnt/QNAP/leefall2/gatk-4.1.0.0/gatk --java-options "-Xmx8G" ApplyBQSR \
--tmp-dir=/mnt/QNAP/leefall2/gatk-4.1.0.0/tmp \
-I '''+sTemporDir+'''/dedup_sorted_'''+sID+'''.bam \
--bqsr-recal-file '''+sTemporDir+'''/recal_'''+str(sID)+'''.table \
-O '''+sOutputDir+'''/'''+str(sID)+'''.bam



''',shell=True)





def RemoveTemporal(sID):


	subprocess.call('''

#Remove temporal data
rm '''+sTemporDir+'''/'''+sID+'''.sam
rm '''+sTemporDir+'''/'''+sID+'''.bam
rm '''+sTemporDir+'''/sorted_'''+sID+'''.bam


''',shell=True)




def Preprocess(value):
		bwa(value)
		Bam_Sort(value)
		MarkDuplicate(value)
		BaseRecalibrate(value)
		RemoveTemporal(value)




if __name__=="__main__":
	StartTime=(time.ctime())
	data_queue=Queue()
	#os.chdir("/mnt/Beta/leefall2/Mesothelioma/downloaded/Symbol_fastq")
#	number_of_cpus=cpu_count()-2
	number_of_cpus=5
	
	manager=Manager()

	p = Pool(number_of_cpus)
	#sFilelist=glob.glob("/mnt/towel/UKBiobank/FE_vcfs/*.gvcf.gz")#4011848
	#lFilelist=glob.glob("*_1.fastq.gz")
	
	lFilelist=list(dPDFileDict.keys())
	lFilelist=lFilelist[:86]
	lFinalFilelist=[]
	for sID in lFilelist:
		if sID in lOutput:
			pass
		else:
			lFinalFilelist.append(sID)
	
	
	
	Pool_map = p.map(Preprocess, lFinalFilelist)

	print("Start Time")
	print(StartTime)
	print("End Time")
	print(time.ctime())
