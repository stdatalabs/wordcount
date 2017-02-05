package com.stdatalabs.SparkWordcount

/*#############################################################################################
# Description: WordCount using Spark
##
# Input: 
#   1. /user/cloudera/MarkTwain.txt
#
# To Run this code use the command:    
# spark-submit --class com.stdatalabs.SparkWordcount.Driver \
#							 --master yarn-cluster \
#							 --num-executors 5 \
#							 --driver-memory 4g \
#							 --executor-memory 4g \
#							 --executor-cores 1 
#							 SparkWordcount-0.0.1-SNAPSHOT.jar \
#							/user/cloudera/MarkTwain.txt \
#							/user/cloudera/sparkWordCount
#############################################################################################*/

// Scala Imports
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.hive.HiveContext

object Driver {

  val conf = new SparkConf().setAppName("Spark - Word Count and Sort in Descending Order")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val file = sc.textFile(args(0))
    val lines = file.map(line => line.replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness) ", " ").toLowerCase())
    val wcRDD = lines.flatMap(line => line.split(" ").filter(_.nonEmpty)).map(word => (word, 1)).reduceByKey(_ + _)
    val sortedRDD = wcRDD.sortBy(_._2, false)

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://quickstart.cloudera:8020"), hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
    } catch {
      case _: Throwable => {}
    }
    sortedRDD.saveAsTextFile(args(1))
  }

}