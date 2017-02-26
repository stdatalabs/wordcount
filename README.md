# MapReduce VS Spark - WordCount Example

Comparing MapReduce to Spark using Wordcount example.

## Requirements
- IDE 
- Apache Maven 3.x
- JVM 6 or 7

## General Info
The repository contains both MapReduce and Spark projects MRWordCount and SparkWordCount
* com/stdatalabs/SparkWordcount
     * Driver.scala --   Spark code to perform wordcount
* com/stdatalabs/mapreduce/wordcount
    * WordCountMapper.java -- Removes special characters from dataset and passes (word, 1) to reducer
    * WordCountReducer.java -- Aggregates values for each key to output wordcount
    * sortingMapper.java -- Receives output from previous MR job and swaps the (K, V) pair
    * sortingComparator.java -- Sorts the mapper output in descending order before passing to reducer
    * sortingReducer.java -- Swaps the (K, V) pair into (word, count) and sends to output file
    * WordCountDriver.java -- Driver program for MapReduce jobs

## Description
* A comparison between MapReduce and Apache Spark RDD code using wordcount example 
  Discussed in blog -- 
     [MapReduce VS Spark - Wordcount Example](http://stdatalabs.blogspot.in/2017/02/mapreduce-vs-spark-wordcount-example.html)

### More articles on hadoop technology stack at [stdatalabs](http://stdatalabs.blogspot.com)

