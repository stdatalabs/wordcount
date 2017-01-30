package com.stdatalabs.mapreduce.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class sortingMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	private Text word;
	private IntWritable count;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] splits = value.toString().split("\\|");
		
		word = new Text(splits[0]);
		count = new IntWritable(Integer.parseInt(splits[1].trim()));
		
		context.write(count, word);
	}

}
