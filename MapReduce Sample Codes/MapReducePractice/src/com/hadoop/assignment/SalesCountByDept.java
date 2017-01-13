/************************************* Aggregation over Composite key *********************/
/* 2.	Total sales in each department of each store */

package com.hadoop.assignment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SalesCountByDept {

	public static class GroupMapper extends Mapper<LongWritable, Text, CompositeKey, IntWritable> {

		CompositeKey salesKey = new CompositeKey();
		Text storeText = new Text();
		Text deptText = new Text();
		IntWritable populat = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] keyvalue = line.split(",");
			storeText.set(new Text(keyvalue[0]));
			deptText.set(keyvalue[1]);
			populat.set(Integer.parseInt(keyvalue[3]));
			CompositeKey salesKey = new CompositeKey(storeText, deptText);
			context.write(salesKey, populat);
		}
	}

	public static class GroupReducer extends Reducer<CompositeKey, IntWritable, CompositeKey, IntWritable> {

		public void reduce(CompositeKey key, Iterable<IntWritable> values, Context context) throws IOException,
		InterruptedException {
			int sum = 0;
			for(IntWritable value : values)
			{
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	private static class CompositeKey implements WritableComparable<CompositeKey> {

		Text store;
		Text dept;

		public CompositeKey(Text store, Text dept) {
			this.store = store;
			this.dept = dept;
		}
		public CompositeKey() {
			this.store = new Text();
			this.dept = new Text();

		}
		public void write(DataOutput out) throws IOException {
			this.store.write(out);
			this.dept.write(out);

		}
		public void readFields(DataInput in) throws IOException {
			this.store.readFields(in);
			this.dept.readFields(in);
		}
		public int compareTo(CompositeKey pop) {
			if (pop == null)
				return 0;
			int intcnt = store.compareTo(pop.store);
			if (intcnt != 0) {
				return intcnt;
			} else {
				return dept.compareTo(pop.dept);
			}
		}
		@Override
		public String toString() {
			return store.toString() + ":" + dept.toString();
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] files=new GenericOptionsParser(conf,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Job job = new Job(conf, "GroupMR");
		job.setJarByClass(SalesCountByDept.class);
		job.setMapperClass(GroupMapper.class);
		job.setReducerClass(GroupReducer.class);
		job.setOutputKeyClass(CompositeKey.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setMaxInputSplitSize(job, 10);
		FileInputFormat.setMinInputSplitSize(job, 100);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
