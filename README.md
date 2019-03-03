# pipeMEM
A tool that run BWA-MEM in Spark

# Pre-Request
```
Anaconda 2018.12
libffi 3.2.1
libgcc-ng 8.2.0
libgcrypt 1.8.4
libgpg-error 1.32
libgsasl 1.8.0
libhdfs3 2.3.0
libntlm 1.4
libprotobuf 3.6.0
libssh2 1.8.0
libstdcxx-ng 8.2.0
libuuid 1.0.3
libxml2 2.9.9
ncurses 6.1
openssl 1.1.1a
pip 18.1
python 2.7.15
readline 7.0
setuptools 40.6.3
sqlite 3.26.0
tk 8.6.8
wheel 0.32.3
xz 5.2.4
zlib 1.2.11
bwa
```
# Download, Compile and comfigure
git clone https://github.com/neozhang307/pipeMEM.git

Please install Anaconda and load requirements.txt in the root directory.

Run
```bash graddlew build ```
to build file 

Please install ```bwa``` and save it in a directory that can be access by all nodes.

Please use ```bwa``` to generate indexes and save them in a directory that can be access by all nodes.

Please mv ```execute_pair_end_mem.sh.sample```(for paired end) or ```execute_single_end_mem.sh.sample ```(for single end) to the the directory that can be access by all nodes. And change the ```bwa``` program location and indexes file location inside.

Please change the ```pipemem.env``` file according to the system environment. 

# Preprocess

After setting ```pipemem.env```, you can run PreProcess.py with:
```
python PreProcess.py -s fasqfile
Or 
python PreProcess.py -p fasqfile1 fastqfile2
```
Please change the fastqfile to where you saved these input files.
# Main Computation
You can either run 
```
python GenerateSparkScript.py
```
To generate script based on the ```pipemem.env``` file or edit the ```runspark.sh.sample``` file directly. 

Please make sure that the ```execute_pair_end_mem.sh.sample```(for paired end) or ```execute_single_end_mem.sh.sample```(for single end) file is putted in a location that every nodes are able to access, and change ```pipemem.env``` or ```runspark.sh.sample``` accordingly.

Run
```bash runspark.sh```
to start the main computation.

# Use AWS with pipeMEM
## create a cluster
You can create a spark cluster in the following address

https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-launch.html

For software configuration, choose Spark

For instance setting, we recommend m5.xlarge

You can use default setting for other part. 
## Use Elastic Storage 
You can create an elastic storage in the following address.

https://aws.amazon.com/efs/

Make sure that the EFS is in the same security group as cluster nodes.

You can mount EFS to every node and access it, following the instructions of Amazon. 

## Create user file in HDFS
There is no user file by default. So please create it with the following command:
```
sudo -u hdfs hadoop fs -mkdir /user/ec2-user
sudo -u hdfs hadoop fs -chown ec2-user /user/ec2-user
```
