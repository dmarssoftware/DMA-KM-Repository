/* 1.	Total sales in each store */

package com.hadoop.assignment;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class SalesCount {
	//Driver class (Public void static main- the entry point)
	public static void main(String [] args) throws Exception
	{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Job j=new Job(c,"salescount");
		j.setJarByClass(SalesCount.class);
		j.setMapperClass(MapForSalesCount.class);
		j.setReducerClass(ReduceForSalesCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}

	//Mapper class -- Map class which extends public class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>  and implements the Map function.
	public static class MapForSalesCount extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
			String content = value.toString(); // read the content of the input file
			System.out.println("content " + content);
			String[] line = content.split("\n"); // split each line by "\n"
		
			for(int i=0 ;i<line.length ;i++ ){ // iterate through each line
				String[] word = line[i].split(","); // split each word by delimiter
				Text outputKey = new Text(word[0].trim()); // construct the key 
				int sale = Integer.parseInt(word[3]);
				IntWritable outputValue = new IntWritable(sale); // construct the value
				con.write(outputKey, outputValue); // execute map by (key,value)
			}
			//P.S - key ,value datatype should match with the input file 
		}
	}
	//Reducer class -Reduce class which extends public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> and implements the Reduce function.
	public static class ReduceForSalesCount extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			// code for aggregation of the value 
			int sum = 0;
			for(IntWritable value : values)
			{
				sum += value.get();
			}
			con.write(word, new IntWritable(sum));
		}
	}
}


/* execute in hadoop cluster :
 * ---------------------------------
 * 
 * 1) make the jar file of the project:
 * 2) put the jar in /home/hduser 
 * 3) put the input file in hdfs ->> hdfs dfs -put inputfile
 * 4) hadoop jar mp.jar com.hadoop.SalesCount /inputfile /output
 * 5) hdfs dfs -ls - /output
 * -rw-r--r--   1 hduser supergroup          0 2016-11-03 14:10 /output/_SUCCESS 
 *-rw-r--r--   1 hduser supergroup         21 2016-11-03 14:10 /output/part-r-00000
 * 6) hdfs dfs -cat /output/part-r-00000

 */
