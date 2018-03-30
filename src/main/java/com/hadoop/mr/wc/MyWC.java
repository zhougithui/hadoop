package com.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyWC {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);

		Job job = Job.getInstance(conf);

		// job.setInputFormatClass(InputFormat.class);
		job.setJarByClass(MyWC.class);
		job.setJobName("myjob");

		Path infile = new Path("/zh/hello.txt");

		//FileInputFormat.setMinInputSplitSize(job, 1);
		//FileInputFormat.setMaxInputSplitSize(job, 1);

		FileInputFormat.addInputPath(job, infile);

		Path outfile = new Path("/data/wc/output01");
		if (outfile.getFileSystem(conf).exists(outfile)) {
			outfile.getFileSystem(conf).delete(outfile, true);
		}
		FileOutputFormat.setOutputPath(job, outfile);

		//map类
		job.setMapperClass(MyMapper.class);
		
		//输出类型和reduce
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(MyReducer.class);

		job.setNumReduceTasks(3);

		job.waitForCompletion(true);

	}

}
