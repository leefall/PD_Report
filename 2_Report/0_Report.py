##-*-coding: utf-8-*-
#!/usr/bin/env python
import gzip
import os, subprocess, glob, time
import sys, time, random, re ,requests, logging, glob
import concurrent.futures
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
import pickle
import xlsxwriter
logger=logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter=logging.Formatter("%(asctime)s - %(message)s")

ch=logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
dCPICSingle=dict()
dCPICDouble=dict()


sIntervarOutputDir="/mnt/Beta/leefall2/PD/Intervar"
sParsedResultDir="/mnt/Beta/leefall2/PD/Report/pypgx"
sReportOutput="/mnt/Beta/leefall2/PD/Report/Final"

with open('Combined.pkl', 'rb') as fr:
	d1KG = pickle.load(fr)

#Parse Single CPIC
fp=open("For_PgXReport_Single.txt", encoding='euc-kr')
fp.readline()
for sLine in fp.readlines():
	sLine=sLine.strip()
	t=sLine.split("\t")
	(sGene, sDrugs, sPhenotype, sDoseRecommendation, sDetailedImplication)=(t[0],t[1],t[3],t[4],t[5])
	sDetailedImplication=sDetailedImplication.replace('"','')
	sDrugs=sDrugs.replace('"','')
	lDrugs=sDrugs.split(",")
	for sDrug in lDrugs:
		if sDrug in dCPICSingle.keys():
			if sGene in dCPICSingle[sDrug].keys():
				dCPICSingle[sDrug][sGene][sPhenotype]=[sDoseRecommendation,sDetailedImplication]
			else:
				dCPICSingle[sDrug][sGene]=dict()
				dCPICSingle[sDrug][sGene][sPhenotype]=[sDoseRecommendation,sDetailedImplication]
		else:
			dCPICSingle[sDrug]=dict()
			dCPICSingle[sDrug][sGene]=dict()
			dCPICSingle[sDrug][sGene][sPhenotype]=[sDoseRecommendation,sDetailedImplication]
			
			
fp.close()



#print(dCPICSingle["Atorvastatin"])
#print(dCPICSingle["Atorvastatin"]["SLCO1B1"])


fp=open("For_PgXReport_Double.txt", encoding='euc-kr')
fp.readline()
for sLine in fp.readlines():
	sLine=sLine.strip()
	t=sLine.split("\t")
	(sGene1, sPhenotype1,sGene2, sPhenotype2, sDrug, sDoseRecommendation, sDetailedImplication)=(t[0],t[1],t[2],t[3],t[4],t[6],t[7])
	sGenePair=sGene1+"_"+sGene2
	sPhenoPair=sPhenotype1+"_"+sPhenotype2
	if sDrug in dCPICDouble.keys():
		if sGenePair in dCPICDouble[sDrug].keys():
			dCPICDouble[sDrug][sGenePair][sPhenoPair]=[sDoseRecommendation,sDetailedImplication]
		else:
			dCPICDouble[sDrug][sGenePair]=dict()
	else:
		
		dCPICDouble[sDrug]=dict()
		dCPICDouble[sDrug][sGenePair]=dict()
		dCPICDouble[sDrug][sGenePair][sPhenoPair]=[sDoseRecommendation,sDetailedImplication]
		
		
fp.close()
	
	
	



class PGxGene:

	def __init__(self):
		
		self.sGene='Null'
		self.sPhenotype='Null'
		self.sGenotype='Null'
		self.sHaplotype1='Null'
		self.sHaplotype2='Null'
		#self.nPhenotype=1000
		
		
		
		#self.sGenotype="
	
		
	def ParseResult(self,sLine):
		
		sLine=sLine.strip()
		t=sLine.split("\t")
		self.sGene=t[0]
		self.sPhenotype=t[1]
		self.sGenotype=t[2]
		try:
			self.sHaplotype1=t[3]
			self.sHaplotype1=self.sHaplotype1.replace(";","")
		except IndexError:
			self.sHaplotype1=t[2].split("/")[0]
		try:
			self.sHaplotype2=t[4]
			self.sHaplotype2=self.sHaplotype2.replace(";","")
		except IndexError:
			self.sHaplotype2=t[2].split("/")[1]
		
		
		
		
		
		
		




def ParsePGx(sFile):
	#os.chdir(sID)
	dPGxdict=dict()
	fp=open(sFile)
	
	#lPGx=["ABCG2","CACNA1S","CFTR","CYP19A1","CYP26A1","CYP2B6","CYP2C19","CYP2C9","CYP2D6","CYP2E1","CYP2F1","CYP2J2","CYP2R1","CYP2S1","CYP2W1","CYP3A4","CYP3A43","CYP3A5","CYP3A7","CYP4B1","CYP4F2","DPYD","G6PD","GSTM1","GSTP1","IFNL3","NAT1","NAT2","NUDT15","POR","RYR1","SLC15A2","SLC22A2","SLCO1B1","SLCO1B3","SULT1A1","TBXAS1","TPMT","UGT1A1","UGT1A4","UGT2B15","UGT2B17","UGT2B7"]
	#lPGx=["ABCG2"]
	
	for sLine in fp.readlines():
		sLine=sLine.strip()
		t=sLine.split("\t")
		cPGx=PGxGene()
		cPGx.ParseResult(sLine)
		dPGxdict[t[0]]=cPGx
		
		
		
		
		
	
	
	return dPGxdict
	
	
	


def CPICAnnotationWrite(dPGxdict,dIntervarPathoDict,dIntervarUSDict,sFile):
	
	sUniqueDrugs=set()
	sUniquePGxs=set()
	sUniqueHDRGenes=set()
	
	
	sID=sFile.split(".")[0]
	workbook = xlsxwriter.Workbook(sReportOutput+"/"+sID+".xlsx")
	Boldformat=workbook.add_format({'bold':True})
	worksheet0 = workbook.add_worksheet("Description")
	worksheet0.set_column('A:B', 80)
	#worksheet1.set_column('C:C', 50)
	n0Row=0
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Summary',Boldformat)
	n0Row+=1
	
	
	worksheet1 = workbook.add_worksheet("Drug Recommendation")
	worksheet1.set_column('A:E', 20)
	worksheet1.set_column('C:C', 50)
	nRow=0
	nCol=0
	#fout=open("/storage/home/leefall2/mypro/CKD/Report/FinalReport/"+sID+".txt","w")
	dNonActionable=dict()
	dActionable=dict()
	
	#dCPICSingle
	#dCPICSingle[sDrug][sGene][sPhenotype]=[sDoseRecommendation,sDetailedImplication]
	#print(dPGxdict)
	
	for sDrug in dCPICSingle.keys():
		
		lTargetGenes=dCPICSingle[sDrug].keys()
		for sTargetGene in lTargetGenes:
		
		
		
		#print(sTargetGene)
		#print(dPGxdict[sTargetGene])
			sSamplePhenotype=dPGxdict[sTargetGene].sPhenotype
			#if ((sTargetGene=="SLCO1B1") and (sDrug=="Atorvastatin")):
#				print(sSamplePhenotype)
			
			
			if sSamplePhenotype in dCPICSingle[sDrug][sTargetGene].keys():
				(sDoseRecommendation,sDetailedImplication)=dCPICSingle[sDrug][sTargetGene][sSamplePhenotype]
				dActionable[sDrug]=[sTargetGene,sDoseRecommendation,sDetailedImplication]
			else:
				dNonActionable[sDrug]=[sTargetGene]
				
	#dCPICDouble
	#dCPICDouble[sDrug][sGenePair][sPhenoPair]=[sDoseRecommendation,sDetailedImplication]
	for sDrug in dCPICDouble.keys():
		lGenePairs=dCPICDouble[sDrug].keys()
		for sGenePair in lGenePairs:
			
			
			(sGene1,sGene2)=sGenePair.split("_")
			
			#sPhenoPair=dCPICDouble[sDrug][sGenePair]
			
			for sPhenoPair in dCPICDouble[sDrug][sGenePair].keys():
				(sDoseRecommendation,sDetailedImplication)=dCPICDouble[sDrug][sGenePair][sPhenoPair]
				
				(sPhenotype1, sPhenotype2)=sPhenoPair.split("_")
				
				sSampleGene1Phenotype=dPGxdict[sGene1].sPhenotype
				sSampleGene2Phenotype=dPGxdict[sGene2].sPhenotype
				
				if ((sSampleGene1Phenotype==sPhenotype1) and (sSampleGene2Phenotype==sPhenotype2)):
					
					dActionable[sDrug]=[sGenePair,sDoseRecommendation,sDetailedImplication]
				else:
					pass
			
		
		
	
	
	lFirst=["Drug","Recommendataion","In detail","Related Gene","Phenotype","Diplotype","Haplotype 1","Haplotype 2","East Asia Proportion","Africa Proportion","America Proportion","Europe Proportion","South Asia Proportion"]
	
	for sCell in lFirst:
		worksheet1.write(nRow,nCol,sCell)
		nCol+=1
	nCol=0
	nRow+=1
	if not dActionable==dict():
		for sDrug in dActionable.keys():
			
			(sTargetGene,sDoseRecommendation,sDetailedImplication)=dActionable[sDrug]
			if not "_"in sTargetGene:
				sSamplePhenotype=dPGxdict[sTargetGene].sPhenotype
				
				sSampleGenotype=dPGxdict[sTargetGene].sGenotype
				sSampleHaplotype1=dPGxdict[sTargetGene].sHaplotype1
				sSampleHaplotype2=dPGxdict[sTargetGene].sHaplotype2
				#d1KG
				#sStarPheno=sStarAllele+"|"+sPhenotype
				#dPgXDict[sGene][sStarPheno][sOneEthnicity]+=1
				sStarPheno=sSampleGenotype+"|"+sSamplePhenotype
				try:
					nEAS=round(d1KG[sTargetGene][sStarPheno]["EAS"]/504,3)
				except KeyError:
					nEAS=0
				try:
					nAFR=round(d1KG[sTargetGene][sStarPheno]["AFR"]/661,3)
				except KeyError:
					nAFR=0
				try:
					nAMR=round(d1KG[sTargetGene][sStarPheno]["AMR"]/347,3)
				except KeyError:
					nAMR=0
				try:
					nEUR=round(d1KG[sTargetGene][sStarPheno]["EUR"]/503,3)
				except KeyError:
					nEUR=0
				try:
					nSAS=round(d1KG[sTargetGene][sStarPheno]["SAS"]/489,3)
				except KeyError:
					nSAS=0

				lOut=[sDrug,sDoseRecommendation,sDetailedImplication,sTargetGene,\
				sSamplePhenotype,sSampleGenotype,sSampleHaplotype1,sSampleHaplotype2,nEAS,nAFR,nAMR,nEUR,nSAS]
				
				
				sUniqueDrugs.add(sDrug)
				sUniquePGxs.add(sTargetGene)
				
				for sCell in lOut:
					worksheet1.write(nRow,nCol,sCell)
					nCol+=1
				nCol=0
				nRow+=1
				
			
			else:
				
				#=sGenePair
				#(sTargetGene,sDoseRecommendation,sDetailedImplication)=dActionable[sDrug]
				(sGene1,sGene2)=sTargetGene.split("_")
				sSampleGene1Phenotype=dPGxdict[sGene1].sPhenotype
				sSampleGene2Phenotype=dPGxdict[sGene2].sPhenotype
				
				
				sSample1Genotype=dPGxdict[sGene1].sGenotype
				sSample1Haplotype1=dPGxdict[sGene1].sHaplotype1
				sSample1Haplotype2=dPGxdict[sGene1].sHaplotype2
				
				sStarPheno=sSample1Genotype+"|"+sSampleGene1Phenotype
				try:
					nEAS1=round(d1KG[sGene1][sStarPheno]["EAS"]/504,3)
				except KeyError:
					nEAS1=0
				try:
					nAFR1=round(d1KG[sGene1][sStarPheno]["AFR"]/661,3)
				except KeyError:
					nAFR1=0
					#print(sGene1)
					#print(sStarPheno)
					#print(d1KG[sGene1][sStarPheno])
					#sys.exit()
				try:
					nAMR1=round(d1KG[sGene1][sStarPheno]["AMR"]/347,3)
				except KeyError:
					nAMR1=0
				try:
					nEUR1=round(d1KG[sGene1][sStarPheno]["EUR"]/503,3)
				except KeyError:
					nEUR1=0
				try:
					nSAS1=round(d1KG[sGene1][sStarPheno]["SAS"]/489,3)
				except KeyError:
					nSAS1=0
				
				
				
				sSample2Genotype=dPGxdict[sGene2].sGenotype
				sSample2Haplotype1=dPGxdict[sGene2].sHaplotype1
				sSample2Haplotype2=dPGxdict[sGene2].sHaplotype2
				
				sStarPheno=sSample2Genotype+"|"+sSampleGene2Phenotype
				try:
					nEAS2=round(d1KG[sTargetGene][sStarPheno]["EAS"]/504,3)
				except KeyError:
					nEAS2=0
				try:
					nAFR2=round(d1KG[sTargetGene][sStarPheno]["AFR"]/661,3)
				except KeyError:
					nAFR2=0
				try:
					nAMR2=round(d1KG[sTargetGene][sStarPheno]["AMR"]/347,3)
				except KeyError:
					nAMR2=0
				try:
					nEUR2=round(d1KG[sTargetGene][sStarPheno]["EUR"]/503,3)
				except KeyError:
					nEUR2=0
				try:
					nSAS2=round(d1KG[sTargetGene][sStarPheno]["SAS"]/489,3)
				except KeyError:
					nSAS2=0
#				

#				
#				
				lOut=[sDrug,sDoseRecommendation,sDetailedImplication,sGene1+","+sGene2,\
				sSampleGene1Phenotype+","+sSampleGene2Phenotype,\
				sSample1Genotype+","+sSample2Genotype,\
				sSample1Haplotype1+","+sSample2Haplotype1,\
				sSample1Haplotype2+","+sSample2Haplotype2,\
				str(nEAS1)+","+str(nEAS2),\
				str(nAFR1)+","+str(nAFR2),\
				str(nAMR1)+","+str(nAMR2),\
				str(nEUR1)+","+str(nEUR2),\
				str(nSAS1)+","+str(nSAS2)]
				
				sUniqueDrugs.add(sDrug)
				sUniquePGxs.add(sGene1)
				sUniquePGxs.add(sGene2)
				
				for sCell in lOut:
					worksheet1.write(nRow,nCol,sCell)
					nCol+=1
				nCol=0
				nRow+=1
				
				
				
				
				
	else:
		worksheet1.write(nRow,nCol,"There is no current actionable PGx variant")
		nRow+=1
		#fout.write("There is no current actionable PGx variant")
	
	###########################################fout.close()############################################
	worksheet2 = workbook.add_worksheet("PGx Info")
	
	worksheet2.set_column('A:E', 20)
	#worksheet1.set_column('C:C', 50)
	nRow=0
	nCol=0
	#fout=open("/storage/home/leefall2/mypro/CKD/Report/PGxInfo/"+sID+".txt","w")
	#fout.write("Pharmacogenomic Gene\tPhenotype\tDiplotype\tHaplotype 1\tHaplotype 2\tEast Asia Proportion\tAfrica Proportion\tAmerica Proportion\tEurope Proportion\tSouth Asia Proportion\n")
	
	lFirst=["Pharmacogenomic Gene","Phenotype","Diplotype","Haplotype 1","Haplotype 2","East Asia Proportion","Africa Proportion","America Proportion","Europe Proportion","South Asia Proportion"]
	#dBoldformat=workbook.add_format({"bold":True})
	for sCell in lFirst:
		worksheet2.write(nRow,nCol,sCell)
		nCol+=1
	nCol=0
	nRow+=1
	
	
	
	sorted(dPGxdict.items())
	
	for sTargetGene in dPGxdict.keys():
	
		
		sSamplePhenotype=dPGxdict[sTargetGene].sPhenotype
		sSampleGenotype=dPGxdict[sTargetGene].sGenotype
		sSampleHaplotype1=dPGxdict[sTargetGene].sHaplotype1
		sSampleHaplotype2=dPGxdict[sTargetGene].sHaplotype2
		#d1KG
		#sStarPheno=sStarAllele+"|"+sPhenotype
		#dPgXDict[sGene][sStarPheno][sOneEthnicity]+=1
		sStarPheno=sSampleGenotype+"|"+sSamplePhenotype
		try:
			nEAS=round(d1KG[sTargetGene][sStarPheno]["EAS"]/504,3)
		except KeyError:
			nEAS=0
		try:
			nAFR=round(d1KG[sTargetGene][sStarPheno]["AFR"]/661,3)
		except KeyError:
			nAFR=0
		try:
			nAMR=round(d1KG[sTargetGene][sStarPheno]["AMR"]/347,3)
		except KeyError:
			nAMR=0
		try:
			nEUR=round(d1KG[sTargetGene][sStarPheno]["EUR"]/503,3)
		except KeyError:
			nEUR=0
		try:
			nSAS=round(d1KG[sTargetGene][sStarPheno]["SAS"]/489,3)
		except KeyError:
			nSAS=0
		#fout.write("{0}\n".format("\t".join(map(str,[sTargetGene,\
		#		sSamplePhenotype,sSampleGenotype,sSampleHaplotype1,sSampleHaplotype2,nEAS,nAFR,nAMR,nEUR,nSAS]))))
		lOut=[sTargetGene,\
				sSamplePhenotype,sSampleGenotype,sSampleHaplotype1,sSampleHaplotype2,nEAS,nAFR,nAMR,nEUR,nSAS]
		for sCell in lOut:
			worksheet2.write(nRow,nCol,sCell)
			nCol+=1
		nCol=0
		nRow+=1
#	
	worksheet3 = workbook.add_worksheet("HDR Report")
	#worksheet3.set_column('A:L', 20)
	worksheet3.set_column('A:M', 20)
	worksheet3.set_column('C:C', 50)
	#worksheet3.set_column('M:M', 50)
	nRow=0
	nCol=0
	
	lFirst=["Gene","Significance","Related Info","HGVS Name","Allelic State","Transcript","AA Change","Region","Chromosome","Position","Reference Allele","Alternative Allele"]
	for sCell in lFirst:
		worksheet3.write(nRow,nCol,sCell)
		nCol+=1
	nCol=0
	nRow+=1
	#fout.write("Gene	Significance	HGVS Name	Allelic State	Transcript	AA Change	Region	Chromosome	Position	Reference Allele	Alternative Allele	rsID	Related Info\n")
	dIntervarNoInfoDict=dict()
	for sKey in dIntervarPathoDict.keys():
		#[sGene,sRsID,sAAChange,sInterVar,sAllelic,sInfo,sHGVS,sNMID,sRsID]
		(sGene,sRsID,sAAChange,sInterVar,sAllelic,sInfo,sHGVS,sNMID)=dIntervarPathoDict[sKey]
		
		(sChr,nPosition,sRef,sAlt)=sKey.split("_")
		
		if sInfo=="":
			dIntervarNoInfoDict[sKey]=(sGene,sRsID,sAAChange,sInterVar,sAllelic,sInfo,sHGVS,sNMID)
			#pass
		elif sInfo=="-":
			dIntervarNoInfoDict[sKey]=(sGene,sRsID,sAAChange,sInterVar,sAllelic,sInfo,sHGVS,sNMID)
		else:
			#fout.write("{0}\n".format("\t".join(map(str,[sGene,sInterVar,sHGVS,sAllelic,sNMID,sAAChange,"HG19",sChr,nPosition,sRef,sAlt,sInfo]))))
			lOut=[sGene,sInterVar,sInfo,sHGVS,sAllelic,sNMID,sAAChange,"HG19",sChr,nPosition,sRef,sAlt]
			sUniqueHDRGenes.add(sGene)
			for sCell in lOut:
				worksheet3.write(nRow,nCol,sCell)
				nCol+=1
			nCol=0
			nRow+=1
			
	
	
	
	#dIntervarUSDict
	#[sGene,sRsID,sAATrans,sInterVar,sAllelic,sInfo]
	worksheet4 = workbook.add_worksheet("UncertainSignifiance")
	#worksheet3.set_column('A:L', 20)
	worksheet4.set_column('A:M', 20)
	worksheet4.set_column('J:J', 50)
	#worksheet3.set_column('M:M', 50)
	nRow=0
	nCol=0
	
	
	lFirst=["Gene","Significance","Allelic State","Region","Chromosome","Position","Reference Allele","Alternative Allele","rsID","Related Info"]
	for sCell in lFirst:
		worksheet4.write(nRow,nCol,sCell)
		nCol+=1
	nCol=0
	nRow+=1
	
	
	for sKey in dIntervarUSDict.keys():
		(sGene,sRsID,sAATrans,sInterVar,sAllelic,sInfo)=dIntervarUSDict[sKey]
		(sChr,nPosition,sRef,sAlt)=sKey.split("_")
		#sUniqueHDRGenes.add(sGene)
		lOut=[sGene,sInterVar,sAllelic,"HG19",sChr,nPosition,sRef,sAlt,sRsID,sInfo]
		for sCell in lOut:
			worksheet4.write(nRow,nCol,sCell)
			nCol+=1
		nCol=0
		nRow+=1
	
	for sKey in dIntervarNoInfoDict.keys():
		(sGene,sRsID,sAAChange,sInterVar,sAllelic,sInfo,sHGVS,sNMID)=dIntervarNoInfoDict[sKey]
		
		(sChr,nPosition,sRef,sAlt)=sKey.split("_")
		lOut=[sGene,sInterVar,sAllelic,"HG19",sChr,nPosition,sRef,sAlt,sRsID,sInfo]
		for sCell in lOut:
			worksheet4.write(nRow,nCol,sCell)
			nCol+=1
		nCol=0
		nRow+=1
		
	
	
	
		
		
#Writeing Descripstion Page

	
	sOutNumberofActionableDrugs=str(len(sUniqueDrugs))+"/"+str(54)
	sOutNumberofActionablePGx=str(len(sUniquePGxs))+"/"+str(43)
	sOutNumberofActionableHDR=str(len(sUniqueHDRGenes))+"/"+str(9244)
	
	worksheet0.write(n0Row,n0Col,'Number of Actionable Drugs / Total Drugs')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,sOutNumberofActionableDrugs)
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Number of Actionable PGx genes / Total PGx (Pharmacogenomic) genes')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,sOutNumberofActionablePGx)
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Number of Actionable HDR genes / Totla HDR (Hereditary Disease Risk) genes')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,sOutNumberofActionableHDR)
	
	n0Row+=1
	n0Col=0
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Drug Recommendation: Drug recommendation guidelines based on actionable PGx genes',Boldformat)
	#n0Col+=1
	#worksheet0.write(n0Row,n0Col,'Drug recommendation guidelines based on actionable PGx genes',Boldformat)
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Column name',Boldformat)
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Description',Boldformat)
	
	
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Drug')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Name of the drug.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Recommendataion')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Guideline by the Clinical Pharmacogenetics Implementation Consortium (CPIC).')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'In detail')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Detailed guideline by the Clinical Pharmacogenetics Implementation Consortium (CPIC). ')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Related Pharmacogenomic Gene')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Related PGx gene for recommendation.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Phenotype')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Observed trait of genotype (e.g. Rapid Metabolizer)')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Diplotype')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Two haplotyes on homologous chrmomosome.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Haplotype 1')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'A group of alleles 1.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Haplotype 2')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'A group of alleles 2.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'East Asia Proportion')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'The proportion of observed diplotype in East asia from 1000 genomes project.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Africa Proportion')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'The proportion of observed diplotype in Africa from 1000 genomes project.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Europe Proportion')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'The proportion of observed diplotype in Europe from 1000 genomes project.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'South Asia Proportion')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'The proportion of observed diplotype in South asia from 1000 genomes project.')
	
	n0Row+=1
	n0Col=0
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'PGx Info: Information of PGx genes.',Boldformat)
	#n0Col+=1
	#worksheet0.write(n0Row,n0Col,'Information of PGx genes.',Boldformat)
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Column name',Boldformat)
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Description',Boldformat)
	
	
	
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Pharmacogenomic Gene')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'The name of the Pharmacogenomic gene.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Phenotype')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Observed trait of genotype (e.g. Rapid Metabolizer)')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Diplotype')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Two haplotyes on homologous chrmomosome.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Haplotype 1')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'A group of alleles 1.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Haplotype 2')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'A group of alleles 2.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'East Asia Proportion')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'The proportion of observed diplotype in East asia from 1000 genomes project.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Africa Proportion')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'The proportion of observed diplotype in Africa from 1000 genomes project.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Europe Proportion')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'The proportion of observed diplotype in Europe from 1000 genomes project.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'South Asia Proportion')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'The proportion of observed diplotype in South asia from 1000 genomes project.')
	
	n0Row+=1
	n0Col=0
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'HDR Report: Information on hereditary disease risk genes variants (Pathogenic, Likely pathogenic)',Boldformat)
	#n0Col+=1
	#worksheet0.write(n0Row,n0Col,'Information on hereditary disease risk genes variants (Pathogenic, Likely pathogenic)',Boldformat)
	
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Column name',Boldformat)
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Description',Boldformat)
	
	
	
	
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Gene')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Gene nomenclature of Human Genome Organisation.')
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Significance')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Clinical significance of variants. (e.g. Pathogenic, Likely pathogenic)')
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Related Info')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Syndrome information associated with the gene from Orphanet.')
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'HGVS Name')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Human Genome Variation Society (HGVS) nomenclature')
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Allelic State')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'The status of allele (e.g. Homozygous, Heterozygous).')
	
	n0Row+=1
	n0Col=0

	worksheet0.write(n0Row,n0Col,'Transcript')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Transcript information with RefSeq transcript ID.')
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'AA Change')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Amino acid change information.')
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Region')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'The version of the used human reference genome. (e.g. Human Genome 19)')
	
	n0Row+=1
	n0Col=0

	
	worksheet0.write(n0Row,n0Col,'Chromosome')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Chromosome name.')
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Position')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Variant position.')
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Reference Allele')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Reference allele information on the position.')
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Alternative Allele')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Alternative allele information on the position.')
	
	n0Row+=1
	n0Col=0
	
	n0Row+=1
	n0Col=0
	
	#################Uncertain Significance
	worksheet0.write(n0Row,n0Col,'UncertainSignifiance: Information on other genes variants. (Likely benign, Benign, Uncertain significance)',Boldformat)
	#n0Col+=1
	#worksheet0.write(n0Row,n0Col,'Information on other genes variants. (Likely benign, Benign, Uncertain significance)',Boldformat)
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Column name',Boldformat)
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Description',Boldformat)
	
	
	
	
	
	n0Row+=1
	n0Col=0
	worksheet0.write(n0Row,n0Col,'Gene')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Gene nomenclature of Human Genome Organisation.')
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Significance')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Clinical significance of variants. (e.g. Pathogenic, Likely pathogenic)')
	
	n0Row+=1
	n0Col=0
	
	
	worksheet0.write(n0Row,n0Col,'Allelic State')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'The status of allele (e.g. Homozygous, Heterozygous).')
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Region')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'The version of the used human reference genome. (e.g. Human Genome 19)')
	
	n0Row+=1
	n0Col=0
	
	
	worksheet0.write(n0Row,n0Col,'Chromosome')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Chromosome name.')
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Position')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Variant position.')
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Reference Allele')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Reference allele information on the position.')
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'Alternative Allele')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Alternative allele information on the position.')
	
	n0Row+=1
	n0Col=0
	
	worksheet0.write(n0Row,n0Col,'rsID')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'The unique label of single nucleotide polymorphism in dbSNP databse.')
	
	n0Row+=1
	n0Col=0

	worksheet0.write(n0Row,n0Col,'Related Info')
	n0Col+=1
	worksheet0.write(n0Row,n0Col,'Related syndrome associated with the gene from Orphanet.')
	
	
	
	
	
	workbook.close()
	


	
	
def ParseInterVar(sFile):
	dIntervarUSDict=dict()
	dIntervarPathoDict=dict()
	sID=sFile.split(".")[0]
	fp=open(sIntervarOutputDir+"/"+sID+".hg19_multianno.txt.intervar")
	fp.readline()
	
	for sLine in fp.readlines():
		sLine=sLine.strip()
		t=sLine.split("\t")
		(sChr,nPosition,sRef,sAlt,sGene,sRsID,sAATrans,sInterVar,sAllelic,sInfo)=\
		(t[0],t[1],t[3],t[4],t[5],t[9],t[11],t[13],t[33],t[-2])
		sInterVar=sInterVar.strip()
		sInterVar=sInterVar.split(" PVS1=")[0]
		sInterVar=sInterVar.split("InterVar: ")[1]
		if "|" in sInfo:
			sInfo=sInfo.split("|")[1]
		sInfo=sInfo.replace("<br>",",")
		
		if sAllelic=="het":
			sAllelic="Heterozygous"
		elif sAllelic=="hom":
			sAllelic="Homozygous"
		#Deletion
		#TMEM52:NM_178545:exon1:c.69_77del:p.23_26del
		#Subsitution
		#SAMD11:NM_152486:exon14:c.C1830T:p.Y610Y
		#Insertion
		#PRDM2:NM_001007257:exon3:c.1501_1502insCTC:p.T501delinsTP,PRDM2:NM_012231:exon8:c.2104_2105insCTC:p.T702delinsTP,PRDM2:NM_015866:exon8:c.2104_2105insCTC:p.T702delinsTP
		sAATrans=sAATrans.split(",")[0]
		
		
		
		
		
		
		
		if ((sInterVar=="Likely pathogenic") or (sInterVar=="Pathogenic")):
			lAATrans=sAATrans.split(":")
			try:
				sNMID=lAATrans[1]
			except IndexError:
				print(lAATrans)
				print(sLine)
				sys.exit()
			nChange=lAATrans[-2]
			sAAChange=lAATrans[-1]
			if "del" in nChange:
				sHGVS=nChange+sAAChange
			elif "ins" in nChange:
				sHGVS=nChange+sAAChange
			else:
				sPosition=nChange[3:-1]
				sHGVS="c."+sPosition+nChange[2]+">"+nChange[-1]
			sKey=sChr+"_"+nPosition+"_"+sRef+"_"+sAlt
			dIntervarPathoDict[sKey]=[sGene,sRsID,sAAChange,sInterVar,sAllelic,sInfo,sHGVS,sNMID]
		else:
			lAATrans=sAATrans.split(":")

			sKey=sChr+"_"+nPosition+"_"+sRef+"_"+sAlt
			dIntervarUSDict[sKey]=[sGene,sRsID,sAATrans,sInterVar,sAllelic,sInfo]
		
		
		
		
		
		
#		
#	
#		
		
	
	return(dIntervarPathoDict,dIntervarUSDict)
	
	
	
	
	


def PGxmodule(sFile):
	

	
	dPGxdict=ParsePGx(sFile)
	(dIntervarPathoDict,dIntervarUSDict)=ParseInterVar(sFile)
	CPICAnnotationWrite(dPGxdict,dIntervarPathoDict,dIntervarUSDict,sFile)
	

	



def producer_task(q, cosmic_dict):

	
	#lFilelist=glob.glob("/storage/home/leefall2/mypro/CKD/Parsed/Merged_VCF/raw/*.vcf")
	lFilelist=glob.glob("*.txt")
	#sID=sFile.split("/")[-1]
	#sID=sID.split(".vc")[0]
	#lFilelist=["CKD0001.txt"]
	for sFile in lFilelist:
		#print(sFilename[-10:])
		#sID=sFile.split("/")[-1]
		#sID=sID.split(".vc")[0]

		value=sFile
		q.put(value)

def consumer_task(q, cosmic_dict):
	while not q.empty():
		value=q.get(True, 0.05)
		

		PGxmodule(value)
		










if __name__=="__main__":
	
	StartTime=(time.ctime())
	data_queue=Queue()
	#os.chdir("/mnt/towel/leefall2/Gustaveraw/EGAF00002385295")
	os.chdir(sParsedResultDir)
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

	
	
	
	
	
	

