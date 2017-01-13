# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 12:57:12 2016

@author: TrishitaC
"""

import sys
import usaddress
from pyspark import SparkContext, SparkConf

IFileName=str(sys.argv[1])
OpFileNm=str(sys.argv[2])
addr1 = int(sys.argv[4])
col_index = addr1 - 1
#addr2 = int(sys.argv[5])

conf = SparkConf().setAppName("AddressApp").setMaster(sys.argv[3])
sc = SparkContext(conf=conf)

addData = sc.textFile(IFileName).cache()

AddressNumber=0
StreetName=1
PlaceName=2
StateName=3
ZipCode=4
AddressNumberPrefix=5
AddressNumberSuffix=6
StreetNamePreDirectional=7
StreetNamePostDirectional=8
StreetNamePreModifier=9
StreetNamePostType=10
StreetNamePreType=11
USPSBoxType=12
USPSBoxID=13
USPSBoxGroupType=14
USPSBoxGroupID=15
LandmarkName=16
CornerOf=17
IntersectionSeparator=18
OccupancyType=19
OccupancyIdentifier=20
SubaddressIdentifier=21
SubaddressType=22
Recipient=23
BuildingName=24

def is_error(content):
    lines = content.strip("\n")
    cols = lines.split(",")
    total_addr = ",".join(cols)
#    inpAddr = str(cols[addr1]) + " " + str(cols[addr2])
    inpAddr = str(cols[col_index])
    parseInpAddr = usaddress.tag(inpAddr)
    FullOpAddress = ["","","","","",
                  "","","","","",
                  "","","","","",
                  "","","","","",
                  "","","","",""]
    for InpKey,InpValue in parseInpAddr[0].iteritems():
		InpValue=str(InpValue)
        	if InpKey=="AddressNumber":
			FullOpAddress[AddressNumber]=InpValue
                elif InpKey=="StreetName":
	             	FullOpAddress[StreetName]=InpValue
               	elif InpKey=="PlaceName":
	        	FullOpAddress[PlaceName]=InpValue
                elif InpKey=="StateName" :
      	                FullOpAddress[StateName]=InpValue
               	elif InpKey=="ZipCode" :
                	FullOpAddress[ZipCode]=InpValue
                elif InpKey=="AddressNumberPrefix" :
       	                FullOpAddress[AddressNumberPrefix]=InpValue
               	elif InpKey=="AddressNumberSuffix" :
                	FullOpAddress[AddressNumberSuffix]=InpValue
                elif InpKey=="StreetNamePreDirectional" :
       	                FullOpAddress[StreetNamePreDirectional]=InpValue
               	elif InpKey=="StreetNamePostDirectional" :
                	FullOpAddress[StreetNamePostDirectional]=InpValue
                elif InpKey=="StreetNamePreModifier" :
       	                FullOpAddress[StreetNamePreModifier]=InpValue
               	elif InpKey=="StreetNamePostType" :
                	FullOpAddress[StreetNamePostType]=InpValue
               	elif InpKey=="StreetNamePreType" :
       	                FullOpAddress[StreetNamePreType]=InpValue
               	elif InpKey=="USPSBoxType" :
                	FullOpAddress[USPSBoxType]=InpValue
                elif InpKey=="USPSBoxID" :
       	        	FullOpAddress[USPSBoxID]=InpValue
               	elif InpKey=="USPSBoxGroupType" :
                	FullOpAddress[USPSBoxGroupType]=InpValue
                elif InpKey=="USPSBoxGroupID" :
       	                FullOpAddress[USPSBoxGroupID]=InpValue
               	elif InpKey=="LandmarkName" :
                	FullOpAddress[LandmarkName]=InpValue
                elif InpKey=="CornerOf" :
       	                FullOpAddress[CornerOf]=InpValue
               	elif InpKey=="IntersectionSeparator" :
                	FullOpAddress[IntersectionSeparator]=InpValue
                elif InpKey=="OccupancyType" :
       	                FullOpAddress[OccupancyType]=InpValue
               	elif InpKey=="OccupancyIdentifier" :
                	FullOpAddress[OccupancyIdentifier]=InpValue
                elif InpKey=="SubaddressIdentifier" :
       	                FullOpAddress[SubaddressIdentifier]=InpValue
               	elif InpKey=="SubaddressType" :
                	FullOpAddress[SubaddressType]=InpValue
                elif InpKey=="Recipient" :
       	                FullOpAddress[Recipient]=InpValue
               	elif InpKey=="BuildingName" :
                	FullOpAddress[BuildingName]=InpValue
    SplitFullOpAddress = ','.join(map(str,FullOpAddress))
    FullRec = total_addr + "," + SplitFullOpAddress
    return FullRec

addsame = addData.map(is_error)

addsame.saveAsTextFile(OpFileNm)
