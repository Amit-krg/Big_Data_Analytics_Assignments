#Name: Amit Gupta
#SBID:110900982
from __future__ import print_function
import re
from pyspark import SparkContext
from operator import add

def myregex(line):
    #print(line)
    exp=r'i feel (\w+)'
    list=re.findall( exp, line, re.I)
    return list

if __name__ == "__main__":
    temp=[]
    sc = SparkContext(appName="Word Count")
    rdd1=sc.textFile("s3://commoncrawl/crawl-data/CC-MAIN-2016-40/wet.paths.gz")
    data=rdd1.collect()

    data=data[0:2]
    for file in data:
        temp.append("s3://commoncrawl/"+file)
    complete_path=','.join(temp) 
    
    lines=sc.textFile(complete_path)
    line=lines.map(lambda s:s.lower())
    final=line.flatMap(lambda s:myregex(s)).map(lambda x: (x, 1)).reduceByKey(add)
    output = final.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))
    output.repartition(1).saveAsTextFile("s3://secondoutput1110/")
    sc.stop()
