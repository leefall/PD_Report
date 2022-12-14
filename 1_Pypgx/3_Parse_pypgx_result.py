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



sPypgxOutputDir="/mnt/Beta/leefall2/PD/pypgx"
sParsedResultDir="/mnt/Beta/leefall2/PD/Report/pypgx"


class PGx:

	def __init__(self):
		
		
		self.dpypgxResults=dict()
		
		self.ABCG2='Null'
		self.CACNA1S='Null'
		self.CFTR='Null'
		self.CYP19A1='Null'
		self.CYP26A1='Null'
		self.CYP2B6='Null'
		self.CYP2C19='Null'
		self.CYP2C9='Null'
		self.CYP2D6='Null'
		self.CYP2E1='Null'
		self.CYP2F1='Null'
		self.CYP2J2='Null'
		self.CYP2R1='Null'
		self.CYP2S1='Null'
		self.CYP2W1='Null'
		self.CYP3A4='Null'
		self.CYP3A43='Null'
		self.CYP3A5='Null'
		self.CYP3A7='Null'
		self.CYP4B1='Null'
		self.CYP4F2='Null'
		self.DPYD='Null'
		self.G6PD='Null'
		self.GSTM1='Null'
		self.GSTP1='Null'
		self.GSTT1='Null'
		self.IFNL3='Null'
		self.NAT1='Null'
		self.NAT2='Null'
		self.NUDT15='Null'
		self.POR='Null'
		self.RYR1='Null'
		self.SLC15A2='Null'
		self.SLC22A2='Null'
		self.SLCO1B1='Null'
		self.SLCO1B3='Null'
		self.SULT1A1='Null'
		self.TBXAS1='Null'
		self.TPMT='Null'
		self.UGT1A1='Null'
		self.UGT1A4='Null'
		self.UGT2B15='Null'
		self.UGT2B17='Null'
		
		
		#self.sGenotype="
	
		
	def ParseResult(self,sGene,sFile):
		self.dpypgxResults[sGene]=dict()
		#sLine=sLine.strip()
		#t=sLine.split("\t")
		fp=open(sFile)
		
		sLine=fp.readline()
		sLine=sLine.strip()
		lFirsts=sLine.split("\t")
		lFirsts=[","]+lFirsts
		sLine=fp.readline()
		sLine=sLine.strip()
		lSeconds=sLine.split("\t")
		lSeconds=lSeconds+[","]
		
		
		
		#print(lFirsts)
		#print(lSeconds)
		
		
		
		for i in range(0,len(lFirsts)):
			self.dpypgxResults[sGene][lFirsts[i]]=lSeconds[i]
			
			
			
	



def ParsePGx(cSample,sID):
	os.chdir(sID)
	
	
	
	lPGx=["ABCG2","CACNA1S","CFTR","CYP19A1","CYP26A1","CYP2B6","CYP2C19","CYP2C9","CYP2D6","CYP2E1","CYP2F1","CYP2J2","CYP2R1","CYP2S1","CYP2W1","CYP3A4","CYP3A43","CYP3A5","CYP3A7","CYP4B1","CYP4F2","DPYD","G6PD","GSTM1","GSTP1","IFNL3","NAT1","NAT2","NUDT15","POR","RYR1","SLC15A2","SLC22A2","SLCO1B1","SLCO1B3","SULT1A1","TBXAS1","TPMT","UGT1A1","UGT1A4","UGT2B15","UGT2B17","UGT2B7"]
	#lPGx=["ABCG2"]
	
	for sGene in lPGx:
		try:
			os.chdir(sGene)
		except FileNotFoundError:
			print("No "+sGene+"~~~~~~~~~~~~~~~")
			print(sID)
			sys.exit()
			
			
			
		try:
			os.system("unzip -n results.zip -d "+sGene)
		except:
			print("No results in "+sGene)
			sys.exit()
		for (sDirpath,sDirname,lFilename) in os.walk("./"+sGene+"/"):
			for sFilename in lFilename:
				if sFilename=="data.tsv":
					sValue=sDirpath+"/"+sFilename
					cSample.ParseResult(sGene,sValue)
					os.chdir("..")
					
					
					
					
	
	
	os.chdir("..")





def WriteResult(cSample,sID):
	
	fout=open(sParsedResultDir+"x/"+sID+".txt","w")
	
	lPGxGene=cSample.dpypgxResults.keys()
	
	for sGene in lPGxGene:
		fout.write("{0}\n".format("\t".join([sGene, cSample.dpypgxResults[sGene]["Phenotype"],\
		cSample.dpypgxResults[sGene]["Genotype"],\
		cSample.dpypgxResults[sGene]["Haplotype1"],\
		cSample.dpypgxResults[sGene]["Haplotype2"]])))
		
		
	
	
	
	
	


def PGxmodule(sID):
	
	cSample=PGx()
	
	ParsePGx(cSample,sID)
	WriteResult(cSample,sID)
	
	
	
	



def producer_task(q, cosmic_dict):

	
	lFilelist=glob.glob("*.vcf.gz")
	
	#sID=sFile.split("/")[-1]
	#sID=sID.split(".vc")[0]
	#lFilelist=["/storage/home/leefall2/mypro/CKD/Parsed/Merged_VCF/raw/CKD0287.vcf"]
	for sFile in lFilelist:
		#print(sFilename[-10:])
		sID=sFile.split("/")[-1]
		sID=sID.split(".vc")[0]

		value=sID
		q.put(value)

def consumer_task(q, cosmic_dict):
	while not q.empty():
		value=q.get(True, 0.05)
		
		#print(value)
		#VariantParse(value)
		#WriteResult(value)
		PGxmodule(value)
		










if __name__=="__main__":
	
	StartTime=(time.ctime())
	data_queue=Queue()
	#os.chdir("/mnt/towel/leefall2/Gustaveraw/EGAF00002385295")
	os.chdir(sPypgxOutputDir)
#	number_of_cpus=cpu_count()-2
	number_of_cpus=10
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

	
	
	
	
	
	

