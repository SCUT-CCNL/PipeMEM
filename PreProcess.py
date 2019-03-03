import os
import time
from multiprocessing import Process,Queue
#from hdfs import InsecureClient
import gc
from hdfs3 import HDFileSystem



def writeHdfs(q,haddress,hport,user,mgfile):
	hdfs = HDFileSystem(host=haddress,port=hport,user=user)
	f = hdfs.open(mgfile,'wb',block_size=134217728)
	f.close()
	f = hdfs.open(mgfile,'ab',buff=112197632)
	while 1:
		seqList = q.get()
		print len(seqList)
		if seqList == 'end':
			break
		#f = hdfs.open('/user/xiaoah/genome/data/NA12878_6000w_merge.fastq','ab')
		f.write(seqList)
		print '1'
	f.close()

def readFile(filename,q_file,splitter):
	f = open(filename,'r')
	seq = ''
	seqList = ''
	id = 1
	print filename,'open success'
#	start = time.time()
	while 1:
		line = f.readline().strip('\n')
	
		if not line:
			break
		if id%4 == 1:
			seq = line
		else:
			seq = seq + splitter+ line
		
		if id%4 ==0:
			seqList += seq	+'\n'	
				
			if id == 223684:
				end = time.time()
#				print "----------------"
#				print "all=",end - start
				q_file.put(seqList)
				seqList=''
				print 'block put'
#				start = time.time()
				id=0
		id = id+1
	q_file.put(seqList)
	q_file.put('end')
	f.close()


def merge(q_file1,q_file2,q_merge,splitter):
	while 1:
		seq1 = q_file1.get()
		seq2 = q_file2.get()
		if seq1 == 'end':
			break
		#print 'merge get '
		list1 = seq1.split('\n')
		list2 = seq2.split('\n')
		content = ''
		i =0
		while i<(len(list1)-1):
			content += list1[i]+splitter+list2[i]+'\n'
			i = i+1
		#print 'merge ok'
		q_merge.put(content)
		print 'merge block put'
	q_merge.put('end')

import ConfigParser
import getpass
import sys

if __name__ == '__main__':

	t_start = time.time()
	q_file1 = Queue(5)
	q_file2 = Queue(5)
	q_merge = Queue(10)


        config = ConfigParser.ConfigParser()
        config.read('pipemem.env')
	address = config.get('PRE','HDFS_HOST')
	#address = 'hdfs://ip-172-31-1-158.ap-northeast-1.compute.internal'
        port = int(config.get('PRE','HDFS_PORT'))
        #port = 8020
        user = getpass.getuser()
        #user = 'ec2-user'
        mgfile = config.get('PRE','MERGE_NAME')
        #the unprintable character from python perspective
        splitter=chr(229)
        if sys.argv[1]=="-p":
	    p1 = Process(target=readFile,args=(sys.argv[2],q_file1,splitter))	
#	    p1 = Process(target=readFile,args=('/data/home/liucheng/data/NA12878_500w_1.fastq',q_file1,))	
	    p1.start()
	    print 'read1 start'

    	    p2 = Process(target=readFile,args=(sys.argv[3],q_file2,splitter))
#           p2 = Process(target=readFile,args=('/data/home/liucheng/data/NA12878_500w_2.fastq',q_file2,))
	    p2.start()
	    print 'read2 start'

	    p3 = Process(target=merge,args=(q_file1,q_file2,q_merge,splitter))
	    p3.start()
	    print 'merge start'

	    p4 = Process(target=writeHdfs,args=(q_merge,address,port,user,mgfile))
	    p4.start()
	    print 'write start'

        if sys.argv[1]=="-s":
	    p1 = Process(target=readFile,args=(sys.argv[2],q_file1,splitter))	
#	    p1 = Process(target=readFile,args=('/data/home/liucheng/data/NA12878_500w_1.fastq',q_file1,))	
	    p1.start()
	    print 'read1 start'

	    p4 = Process(target=writeHdfs,args=(q_file1,address,port,user,mgfile))
	    p4.start()
	    print 'write start'

