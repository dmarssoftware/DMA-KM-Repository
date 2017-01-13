package com.distributedcache.MaxMinSale;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text,DoubleWritable,Text,Text>{
	protected void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
	
		double max = 0.0;
		double min = 0.0;
		min = Double.MAX_VALUE;
		DoubleWritable maxWord = new DoubleWritable();
		DoubleWritable minword = new DoubleWritable();
		
		for(DoubleWritable value : values)
		{
			if(value.get() > max){
	            max = value.get();
	            maxWord.set(max);
	        }
			if(value.get() < min){
				min = value.get();
				minword.set(min);
			}
		}	
		Text maxmin = new Text("");
		maxmin.set(maxWord+":"+minword);
		context.write(key,maxmin);
	}
}