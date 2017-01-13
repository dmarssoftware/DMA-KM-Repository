package com.variationsOfsales.CPI;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
	protected void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		double sum = 0;
		for(DoubleWritable value : values)
		{
			sum += value.get();
		}
		context.write(key, new DoubleWritable(sum));

	}
}