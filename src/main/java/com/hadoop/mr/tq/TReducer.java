package com.hadoop.mr.tq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TReducer extends Reducer<TQ, IntWritable, Text, IntWritable>{
	

	Text rkey = new Text();
	IntWritable rval = new IntWritable();
	
	
	@Override
	protected void reduce(TQ key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		//key:
		//1970,01,08,88
		//1970,01,08,82
		//1970,01,01,80
		//....
		//vals:
		//88
		//82
		//80
		int flg = 0;
		int day = 0;
		for (IntWritable val : values) {
			if(flg==0){
				
				rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
				rval.set(key.getWd());
				day=key.getDay();
				context.write(rkey, rval);
				flg++;
			}
			if(flg!=0 && day != key.getDay()){
				
				rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
				rval.set(key.getWd());
				context.write(rkey, rval);
				break;
			}
			
			
		}
	}
}
