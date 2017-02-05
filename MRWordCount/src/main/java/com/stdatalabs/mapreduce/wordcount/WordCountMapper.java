package com.stdatalabs.mapreduce.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString().replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness) ", " ").toLowerCase();

		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());			
			context.write(word, one);
		}
	}

}