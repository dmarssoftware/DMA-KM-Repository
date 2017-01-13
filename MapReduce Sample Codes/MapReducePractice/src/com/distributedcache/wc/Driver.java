package com.distributedcache.wc;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Driver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("Usage: %s needs two arguments    files\n",
					getClass().getSimpleName());
			return -1;
		}
		Job job = new Job();
		job.setJarByClass(Driver.class);
		job.setJobName("Word Counter With Stop Words Removal");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());
		int returnValue = job.waitForCompletion(true) ? 0:1;
		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");          
		}
		return returnValue;
	}
}


/* hduser@hadoop-master:~$ hdfs dfs -cat /input
16/11/07 15:56:31 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
box,nivedita,chair,table,software,box,chair,is,a,to,to,the,ok,software,to

*hduser@hadoop-master:~$ hdfs dfs -cat /stop
16/11/07 15:57:23 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
is
a
the
to
ok

hadoop jar wc.jar com.distributedcache.Driver /input /outputCache2 /stop
*/