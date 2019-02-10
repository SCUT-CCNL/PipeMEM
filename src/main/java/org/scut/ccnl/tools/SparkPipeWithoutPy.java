package org.scut.ccnl.tools;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

//import org.scut.ccnl.Utils.*;
//import org.scut.ccnl.WriteReads;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.ArrayList;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;


public class SparkPipeWithoutPy {
   	public static Iterator<String> splitPE(String record){
        	List<String> seq = Arrays.asList(record.split("\\|"));
       		return seq.iterator();
    	}

	public static void WriteReads(final SparkSession spark, final String outputPath, final JavaRDD<String> read) throws Exception{
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path newFolderPath = new Path(outputPath);
        if (hdfs.exists(newFolderPath)) {
            hdfs.delete(newFolderPath, true);
        }
        
        read.saveAsTextFile(outputPath); 
    }

	 public static void main(String[] args) throws Exception {


        String extern_program = args[0];
        String inputFile = args[1];
        String outputPath = args[2];
       // String format = args[3];
       // String dictionaryPath = args[4];
        //iString rg = args[5];
        

//        String extern_program = "/home/jryoung/documents/cuda_mem_testdata/extern_pro.sh";
//        String inputFile = "/home/jryoung/documents/cuda_mem_testdata/chr3_2_merge.fastq";
//        String outputPath = "/home/jryoung/documents/cuda_mem_testdata/test2_parquet/";
//        String format = "parquet";
//        String dictionaryPath = "/home/jryoung/documents/cuda_mem_testdata/chr3.dict";
//        String rg = "4|lib1|illumina|20|22";
        SparkConf conf = new SparkConf().setAppName("SparkPipeWithoutPy");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        
        JavaRDD<String> mergedRDD = spark.read().textFile(inputFile).javaRDD();
        System.out.println("Partitions num:" + mergedRDD.getNumPartitions());

        JavaRDD<String> splitRDD = mergedRDD.flatMap(pair -> splitPE(pair));

        JavaRDD<String> alignedRecord = splitRDD.pipe(extern_program);


        WriteReads(spark,outputPath,alignedRecord);

    }

}




