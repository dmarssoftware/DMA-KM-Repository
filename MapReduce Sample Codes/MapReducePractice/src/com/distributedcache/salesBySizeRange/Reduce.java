package com.distributedcache.salesBySizeRange;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
	protected void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		DoubleWritable sales = new DoubleWritable();
		double sum = 0;
		for(DoubleWritable value : values){
			sum += value.get();
		}
		sales.set(sum);
		context.write(key, sales);

	}
}