package com.stdatalabs.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	public void reduce(final Text key, final Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

		int sum = 0;
		
		for (IntWritable value : values) {
			sum = sum + value.get();
		}
       
		context.write(key, new IntWritable(sum));
	}

}