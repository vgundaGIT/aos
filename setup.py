#!/usr/bin/python3

import os
import shutil
import argparse

parser = argparse.ArgumentParser(description="Creates test bed.")
parser.add_argument('NumClients','--num',help= "Number of peers to be running")
args = parser.parse_args()

numClients = args.NumClients

print(numClients)

# testDir = "test_dir"

# #Remove the files that exist
# if os.path.isdir(testDir):
#     shutil.rmtree(testDir)

# #create test dir
# os.mkdir(testDir)

#Take input argument on number of directories to create


#Create directories



#Create files using dd command












