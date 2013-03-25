package com.cloudera.example.chain.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExampleDriver implements Tool {

	private Configuration conf;
	
	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(new Configuration(false));
		JobConf empty = new JobConf(new Configuration(false));
		job.setMapperClass(ChainMapper.class);
		ChainMapper.addMapper(job, FirstMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, empty);
		ChainMapper.addMapper(job, SecondMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, empty);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		
		job.setJarByClass(ExampleDriver.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		RunningJob running = JobClient.runJob(job);
		running.waitForCompletion();
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ExampleDriver chain = new ExampleDriver();
		int result = ToolRunner.run(chain, args);
		System.exit(result);
	}
}
