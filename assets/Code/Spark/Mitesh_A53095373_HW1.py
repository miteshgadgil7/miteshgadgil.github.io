# Name: Mitesh Gadgil	
# Email: mgadgil@ucsd.edu
# PID: A53095373

from pyspark import SparkContext
sc = SparkContext()

textRDD = sc.newAPIHadoopFile('/data/Moby-Dick.txt',
                              'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
                              'org.apache.hadoop.io.LongWritable',
                              'org.apache.hadoop.io.Text',
                               conf={'textinputformat.record.delimiter': "\r\n\r\n"}) \
            .map(lambda x: x[1])

sentences=textRDD.flatMap(lambda x: x.split(". "))	

def printOutput(n,freq_ngramRDD):
    top=freq_ngramRDD.take(5)
    print '\n============ %d most frequent %d-grams'%(5,n)
    print '\nindex\tcount\tngram'
    for i in range(5):
        print '%d.\t%d: \t"%s"'%(i+1,top[i][0],top[i][1])


import re		
# Function that removes punctuations and converts text to lower case
def pprocess(sentence):
	return ' '.join(filter(lambda x: x!= '',re.sub(r'[^[\w\s]|\r|\n]',' ',sentence).lower().split(" ")))
		
sent_processed = sentences.map(pprocess)
		
for n in range(1,6):
    # function that maps(flatmap) each sentence to the list of possible n-grams
	def ngram(x):
		if n==1: return x.split(" ")
		elif n==2: return [' '.join([x.split(" ")[i-1],x.split(" ")[i]]) for i in range(1,len(x.split(" ")))]
		elif n==3: return [' '.join([x.split(" ")[i-2],x.split(" ")[i-1],x.split(" ")[i]]) for i in range(2,len(x.split(" ")))]
		elif n==4: return [' '.join([x.split(" ")[i-3],x.split(" ")[i-2],x.split(" ")[i-1],x.split(" ")[i]]) for i in range(3,len(x.split(" ")))]
		elif n==5: return [' '.join([x.split(" ")[i-4],x.split(" ")[i-3],x.split(" ")[i-2],x.split(" ")[i-1],x.split(" ")[i]]) for i in range(4,len(x.split(" ")))]
	
	# Aggregating the no. of occurences of the n-gram and sorting in descending order
	freq_ngramRDD = sent_processed.flatMap(ngram)\
									.map(lambda gram: (gram,1))\
									.reduceByKey(lambda a,b: a+b)\
									.map(lambda (c,v): (v,c))\
									.sortByKey(False)

    # Output printed									
	printOutput(n,freq_ngramRDD)