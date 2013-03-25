package com.cloudera.example.chain.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FirstMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter arg3) throws IOException {
		String line = value.toString();
		String nice = line.replaceAll("\\.", ", which was nice.");
		value.set(nice);
		output.collect(key, value);
	}

}
