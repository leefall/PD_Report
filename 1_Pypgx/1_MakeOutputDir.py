#!/usr/bin/env python

import glob
import os

lFilelist=glob.glob("/mnt/Beta/leefall2/PD/VCF/Filter/*.vcf.gz")

for sFile in lFilelist:
	sID=sFile.split("/")[-1]
	sID=sID.split(".vc")[0]
	os.mkdir("/mnt/Beta/leefall2/PD/pypgx/"+sID)




