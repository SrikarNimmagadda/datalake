# live tester for cluster_finder
# you need to have your aws credentials set up
# and pass the stack name of the emr cluster stack on the command line

import sys
import boto3
from cluster_finder import cluster_finder

print 'Looking for ClusterId output of CloudFormation stack "' + sys.argv[1] + '".'

CFN = boto3.client('cloudformation')

finder = cluster_finder(CFN)
clusterid = finder.find_cluster(sys.argv[1])

print clusterid
