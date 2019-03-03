import os
import time


import ConfigParser
import getpass
import sys

if __name__ == '__main__':


        config = ConfigParser.ConfigParser()

        config.read('pipemem.env')
        user = getpass.getuser()
        mgfile = config.get('PIPE','MERGE_NAME')
        rstfile = config.get('PIPE','RESULT_DIR')
        basedir = config.get("PIPE","USER_DIR")
        
        exe_core = config.get("PIPE","EXE_CORE")
        exe_mmr = config.get("PIPE","EXE_MEMORY")
        
        executordir = config.get("PIPE","EXECUTOR")
       
        f=open("runspark.sh","w+")
        f.write(" spark-submit --class org.scut.ccnl.tools.SparkPipeWithoutPy --master yarn --deploy-mode cluster --executor-cores %s --executor-memory %s ~/pipeMEM/build/libs/pipeMEM-1.0-SNAPSHOT.jar %s %s %s"  \
  % (exe_core,exe_mmr, \
    executordir, basedir+"/"+mgfile, basedir+"/"+rstfile))

