hdfs dfs -rm -r /ER
cd EntityResolution_Input_Files/
rm *
cd

############### Starting Phase ###################

hdfs dfs -mkdir /ER
hdfs dfs -mkdir /ER/input_files

cp US_Restaurants.csv EntityResolution_Input_Files/
cd EntityResolution_Input_Files/
awk '{print FILENAME"_"NR","$0 }' US_Restaurants.csv > US_Restaurants.csv.bkp
mv US_Restaurants.csv.bkp US_Restaurants.csv
cd
hdfs dfs -put EntityResolution_Input_Files/* hdfs://hadoop-master:54310/ER/input_files

############### Preprocessing ##############

hdfs dfs -rm -r hdfs://hadoop-master:54310/ER/dataQualityOutput
spark-submit --master spark://hadoop-master:7077 --class com.rs.Preprocessing.DataQualityValidation EntityResolution_JARS/Entity_Resolution.jar hdfs://hadoop-master:54310/ER/input_files/ hdfs://hadoop-master:54310/ER/dataQualityOutput ,
hdfs dfs -cat /ER/dataQualityOutput/part* | hdfs dfs -put - hdfs://hadoop-master:54310/ER/dataQualityOutput/US_Restaurants.csv
hdfs dfs -rm /ER/dataQualityOutput/part*
hdfs dfs -rm /ER/dataQualityOutput/_*

############### Address Segmentation ################

hdfs dfs -rm -r hdfs://hadoop-master:54310/ER/addr_segment/
spark-submit address_segmentation.py hdfs://hadoop-master:54310/ER/dataQualityOutput hdfs://hadoop-master:54310/ER/addr_segment spark://hadoop-master:7077 5
#hdfs dfs -rm -r hdfs://hadoop-master:54310/tmp_output_file/
#hdfs dfs -mkdir hdfs://hadoop-master:54310/tmp_output_file/
#hdfs dfs -cat hdfs://hadoop-master:54310/ER/addr_segment/part* | hdfs dfs -put - hdfs://hadoop-master:54310/tmp_output_file/US_Restaurants.csv
hdfs dfs -cat hdfs://hadoop-master:54310/ER/addr_segment/part* | hdfs dfs -put - hdfs://hadoop-master:54310/ER/addr_segment/US_Restaurants.csv
hdfs dfs -rm /ER/addr_segment/part*
hdfs dfs -rm /ER/addr_segment/_*

##################### Indexing ###############

hdfs dfs -rm -r hdfs://hadoop-master:54310/ER/index_temp/*
hdfs dfs -rm -r hdfs://hadoop-master:54310/ER/index_output/
spark-submit --master spark://hadoop-master:7077 --class com.rs.Indexing.Main EntityResolution_JARS/Entity_Resolution.jar hdfs://hadoop-master:54310/ER/addr_segment/ hdfs://hadoop-master:54310/ER/index_output/ hdfs://hadoop-master:54310/ER/index_temp/ "US_Restaurants.csv|3,4:4,4" 3 40 , hdfs://hadoop-master:54310

#################### Matching ################

hdfs dfs -rm -r hdfs://hadoop-master:54310/ER/match_output/
spark-submit --master spark://hadoop-master:7077 --class com.rs.Matching.MainObject EntityResolution_JARS/Entity_Resolution.jar hdfs://hadoop-master:54310/ER/index_output/part* hdfs://hadoop-master:54310/ER/match_output/ "US_Restaurants.csv|5:levenshtein" ,

################### Intermediary Consolidation ###############

hdfs dfs -rm hdfs://172.18.19.155:54310/affinity/StructredOutputDataV11.csv
#hdfs dfs -rm -r hdfs://hadoop-master:54310/tmp_output_file/
hdfs dfs -cat hdfs://hadoop-master:54310/ER/match_output/part* | hdfs dfs -put - hdfs://172.18.19.155:54310/affinity/StructredOutputDataV11.csv
hdfs dfs -rm hdfs://172.18.19.155:54310/affinity/ColumnWeightV9.csv
echo _c6,AddrScore,1.0 > ColumnWeightV9.csv
#echo _c9,RestScore,0.3 > ColumnWeightV9.csv
hdfs dfs -put ColumnWeightV9.csv hdfs://172.18.19.155:54310/affinity/
hdfs dfs -cat hdfs://172.18.19.155:54310/affinity/ColumnWeightV9.csv

################# R Server Start #########
######## Classification ###########

ssh 172.18.19.155
cd affinity/
Rscript affinity_sparkr.r
sed -n '/Exemplars:/,/Clusters:/p' ClusterOutput.txt | sed 's/Exemplars://g' | sed 's/Clusters://g' | sed '/^$/d' > structuredOp.txt
scp structuredOp.txt 172.18.100.103:/home/hduser/
exit

########## R Server End ###########

######## Deduplication ############

hdfs dfs -mkdir /ER/UniqueRecord/
cd
hdfs dfs -put structuredOp.txt /ER/UniqueRecord/
spark-submit --master spark://hadoop-master:7077 --class com.rs.Deduplication.UniqueRecordGenerate EntityResolution_JARS/Entity_Resolution.jar hdfs://hadoop-master:54310/ER/UniqueRecord/structuredOp.txt hdfs://hadoop-master:54310/ER/input_files/US_Restaurants.csv hdfs://hadoop-master:54310/ER/UniqueRecord/joinSet
hdfs dfs -cat /ER/UniqueRecord/joinSet/part* | head -10

############# END #############