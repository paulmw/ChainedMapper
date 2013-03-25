package com.cloudera.example.oozie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.oozie.action.hadoop.MapReduceMain;

import com.cloudera.example.chain.mapred.FirstMapper;
import com.cloudera.example.chain.mapred.SecondMapper;

public class CustomLauncher extends MapReduceMain {

	@Override
	protected RunningJob submitJob(Configuration conf) throws Exception {
		JobConf job = new JobConf(conf);
		job.setMapperClass(ChainMapper.class);
		ChainMapper.addMapper(job, FirstMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, job);
		ChainMapper.addMapper(job, SecondMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, job);	
		return super.submitJob(job);
	}

	public static void main(String[] args) throws Exception {
		run(CustomLauncher.class, args);
	}

}
