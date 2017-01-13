package com.distributedcache.salesBySizeRange;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private static HashMap<String, String> DepartmentMap = new HashMap<String, String>();
	private BufferedReader brReader;
	private String store = "";
	private String storeVal = "";
//	private long store ;
	private Text txtMapOutputKey = new Text("");
	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (cacheFilesLocal != null && cacheFilesLocal.length > 0) {
			for (Path eachPath : cacheFilesLocal) {
				loadDepartmentsHashMap(eachPath, context);
			}
		}
	}
	private void loadDepartmentsHashMap(Path filePath, Context context) throws IOException {
		try {
			String strLineRead = null;
			brReader = new BufferedReader(new FileReader(filePath.toString()));
			// Read each line, split and load to HashMap
			while ((strLineRead = brReader.readLine()) != null) {
				String deptFieldArray[] = strLineRead.split("\t");
				DepartmentMap.put(deptFieldArray[0].trim(),deptFieldArray[2].trim());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public String setStoreSize(long storeSize){
		if(storeSize<=100000){
			return "Size<1 lakhs------>";
		}
		else if(storeSize>100000 && storeSize<=150000){
			return "1 lakh<Size<2 lakhs--------> ";
		}
		else return "above 1.5 lakh--------->";
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String content = value.toString();
		String line[] = content.split("\n");
		DoubleWritable Output_value = new DoubleWritable();
		for(int i = 0; i< line.length; i++ ){
			String arrEmpAttributes[] = line[i].split("\t");
			try{
				store = DepartmentMap.get(arrEmpAttributes[0]);
				Long stores = Long.parseLong(store);
				storeVal = setStoreSize(stores);
			} 
			finally{
				storeVal = ((storeVal.equals(null) || storeVal.equals("")) ? "NOT-FOUND" : storeVal);
			}
			txtMapOutputKey.set(storeVal);
		//	Output_value.set(Double.parseDouble(arrEmpAttributes[3]));
			Output_value.set(Double.parseDouble(arrEmpAttributes[3]));
		}
		context.write(txtMapOutputKey, Output_value);
	}
}