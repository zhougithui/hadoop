package com.hadoop.mr.tq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyTQ {
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration(true);
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MyTQ.class);
//		job.setInputFormatClass(ooxx.class);
		
		//mapTask
		job.setMapperClass(TMapper.class);
		job.setMapOutputKeyClass(TQ.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(TPartitioner.class);
		
		job.setSortComparatorClass(TSorter.class);
		
//		job.setCombinerClass(TCobiner.class);
		
		//reduceTask
		job.setGroupingComparatorClass(TGroupingCoparator.class);
		
		job.setReducerClass(TReducer.class);
		
		job.setNumReduceTasks(2);
		
		
		//input&output
		
		Path infile = new Path("/data/tq/input");
		FileInputFormat.addInputPath(job, infile );
		
		
		Path outfile = new Path("/data/tq/output");
		if(outfile.getFileSystem(conf).exists(outfile)){
			outfile.getFileSystem(conf).delete(outfile,true);
		}
		FileOutputFormat.setOutputPath(job, outfile );
		
		
		
		
		job.waitForCompletion(true);
		
		
		
		
	}
	
	
	
	
	
	

}
