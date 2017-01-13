package com.distributedcache.salesCount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MapperClass extends Mapper<LongWritable,Text,Text,IntWritable>{

	//private final static IntWritable one = new IntWritable(1);
	//private Text word = new Text();
	private Set<String> stopWords = new HashSet<String>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		try{
			Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if(stopWordsFiles != null && stopWordsFiles.length > 0) {
				for(Path stopWordFile : stopWordsFiles) {
					readFile(stopWordFile);
				}
			}
		} catch(IOException ex) {
			System.err.println("Exception in mapper setup: " + ex.getMessage());
		}
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String content = value.toString(); 
		String[] line = content.split("\n"); 
		for(int i=0 ;i<line.length ;i++ ){ 
			String[] word = line[i].split(","); 
			if(stopWords.contains(word[0])){
			Text outputKey = new Text(word[0].trim()); 
			int	sale = Integer.parseInt(word[3]);
				IntWritable outputValue = new IntWritable(sale);
				System.out.println(outputKey+","+outputValue);
				context.write(outputKey, outputValue); 
			}
		}
	}

	@SuppressWarnings("resource")
	private void readFile(Path filePath) {
		try{
			BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
			String stopWord = null;
			while((stopWord = bufferedReader.readLine()) != null) {
				stopWords.add(stopWord.toLowerCase());
			}
		} catch(IOException ex) {
			System.err.println("Exception while reading stop words file: " + ex.getMessage());
		}

	}

}
