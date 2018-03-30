package com.hadoop.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();
	// hello 1
	// hello 1
	// hello 1
	// hello 1
	// hello 1

	// bj.hd.sxt 11
	// bj.cp.bd 22
	// bj.hd.qh 33
	// sh.hq.jd
	// sh.pd.oo
	// sh.hq.sxt

	public void reduce(Text key /* hello */,
			Iterable<IntWritable> values, /* 1,1,1,1,1 */
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			System.out.println(val);
			// 11
			// 22
			// 33
			System.out.println(key);
			// bj.hd.sxt
			// bj.cp.bd
			// bj.hd.qh
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}

}
