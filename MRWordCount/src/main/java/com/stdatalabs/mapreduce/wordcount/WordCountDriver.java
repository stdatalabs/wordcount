package com.stdatalabs.mapreduce.wordcount;

/*#############################################################################################
# Description: WordCount using Map Reduce
##
# Input: 
#   1. /user/cloudera/MarkTwain.txt
#
# To Run this code use the command:    
# yarn jar wordcount-0.0.1-SNAPSHOT.jar \
#		   com.stdatalabs.mapreduce.wordcount.WordCountDriver \
#		   MarkTwain.txt \
#		   mrWordCount \
#		   mrWordCount_sorted
#############################################################################################*/

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: [input] [output1] [output2]");
			System.exit(-1);
		}
		Job wordCount = Job.getInstance(getConf());
		wordCount.setJobName("wordcount");
		wordCount.setJarByClass(WordCountDriver.class);
		
		/* Field separator for reducer output*/
		wordCount.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");
		
		
		wordCount.setOutputKeyClass(Text.class);
		wordCount.setOutputValueClass(IntWritable.class);

		wordCount.setMapperClass(WordCountMapper.class);
		wordCount.setCombinerClass(WordCountReducer.class);
		wordCount.setReducerClass(WordCountReducer.class);

		wordCount.setInputFormatClass(TextInputFormat.class);
		wordCount.setOutputFormatClass(TextOutputFormat.class);
		
		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);
		
		FileInputFormat.addInputPath(wordCount, inputFilePath);
		FileOutputFormat.setOutputPath(wordCount, outputFilePath);
		
		/* Delete output filepath if already exists */
		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
		
		wordCount.waitForCompletion(true);
		
		Job sort = Job.getInstance(getConf());
		sort.setJobName("WordCount Sorting");
		sort.setJarByClass(WordCountDriver.class);
		
		/* Field separator for reducer output*/
		sort.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");
		
		
		sort.setOutputKeyClass(Text.class);
		sort.setOutputValueClass(IntWritable.class);

		sort.setMapperClass(sortingMapper.class);
		sort.setReducerClass(sortingReducer.class);
		sort.setSortComparatorClass(sortingComparator.class);
		
		sort.setMapOutputKeyClass(IntWritable.class);
		sort.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(sort, new Path(args[1]));
		FileOutputFormat.setOutputPath(sort, new Path(args[2]));

		if (fs.exists(new Path(args[2]))) {
			fs.delete(new Path(args[2]), true);
		}
		
		return sort.waitForCompletion(true) ? 0 : 1;
	}
	

	public static void main(String[] args) {
		WordCountDriver wordcountDriver = new WordCountDriver();
		int res;
		try {
			res = ToolRunner.run(wordcountDriver, args);
			System.exit(res);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

