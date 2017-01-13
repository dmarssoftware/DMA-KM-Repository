/*********************** 9.  Max and min sales of each store, dept and type   ***********************/

package com.distributedcache.MaxMinSale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapSideJoin extends Configured implements Tool {

  @Override
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.printf("Two parameters are required- <input dir> <output dir>\n");
			return -1;
		}
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		job.setJobName("Map-side join with text lookup file in DCache");
		DistributedCache.addCacheFile(new Path(args[2]).toUri(),conf);
		job.setJarByClass(MapSideJoin.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		//job.setNumReduceTasks(0);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),new MapSideJoin(), args);
		System.exit(exitCode);
	}
}
/***************** OUTPUT*************
10:1:B  17508.41:15381.82
1:1:A   46039.49:19403.54
2:1:A   22136.64:21043.39
3:1:B   57258.43:17596.96
4:1:A   17413.94:16145.35
5:1:B   18926.74:18926.74
6:1:A   15580.43:14773.04
7:1:B   17558.09:16637.62
8:1:A   16333.14:16216.27
9:1:B   17688.76:15360.45
********/