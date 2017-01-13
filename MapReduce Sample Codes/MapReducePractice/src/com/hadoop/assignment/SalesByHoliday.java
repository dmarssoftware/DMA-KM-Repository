/************** 3.	Total sales on Holidays and Non Holidays  *********************/

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
public class SalesByHoliday {
	
	public static void main(String [] args) throws Exception
	{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Job j=new Job(c,"salescount");
		j.setJarByClass(SalesByHoliday.class);
		j.setMapperClass(MapForSalesCount.class);
		j.setReducerClass(ReduceForSalesCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}

	
	public static class MapForSalesCount extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
			String content = value.toString(); 
			System.out.println("content " + content);
			String[] line = content.split("\n"); 
		
			for(int i=0 ;i<line.length ;i++ ){ 
				String[] word = line[i].split(","); 
				Text outputKey = new Text(word[4].trim()); 
				String str = outputKey.toString();
				if(str.equalsIgnoreCase("FALSE")){
					outputKey.set("NON-HOLIDAY");
				}
				else{
					outputKey.set("HOLIDAY");
				}
				int sale = Integer.parseInt(word[3]);
				IntWritable outputValue = new IntWritable(sale); 
				con.write(outputKey, outputValue); 
			}
			 
		}
	}
	
	public static class ReduceForSalesCount extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			
			int sum = 0;
			for(IntWritable value : values)
			{
				sum += value.get();
			}
			con.write(word, new IntWritable(sum));
		}
	}
}

