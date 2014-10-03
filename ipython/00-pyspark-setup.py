import os
os.environ['SPARK_HOME'] = '/root/spark/'
import sys
sys.path.insert(0, '/root/spark/python')
CLUSTER_URL = open('/root/spark-ec2/cluster-url').read().strip()
from pyspark import  SparkContext
sc = SarkContext( CLUSTER_URL, 'pyspark')
