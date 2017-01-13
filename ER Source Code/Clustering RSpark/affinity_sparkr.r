Sys.setenv(SPARK_HOME = "/usr/local/spark")
Sys.setenv(HADOOP_CMD="/usr/local/hadoop/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/local/hadoop/share/hadoop/tools/lib/hadoopstreaming-2.7.2.jar")

.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))

library(qdapTools)
library(apcluster)
library(dplyr)    
library(stringr)
library(rhdfs)
library(SparkR)

setwd("/home/hduser/affinity")
hdfs.init()
#gc()

sc <- sparkR.init(master = "spark://hadoop-master:7077", appName = "RCode")
#sc <- sparkR.init(master = "spark://hadoop-master:7077",sparkEnvir = list(spark.driver.memory="2g"),appName = "RCode")
sqlContext <- sparkRSQL.init(sc)

if (file.exists('ClusterOutput.txt')){
  file.remove('ClusterOutput.txt')
}


###Below file is output of indexing process.It has column from source1 ,Source2 and the levinstein distance score for each combination of columns
#StrctOp <- read.csv(file="StructredOutputDataV9.csv", header=TRUE, sep=",")  

StrctOp = read.df("hdfs://hadoop-master:54310/affinity/StructredOutputDataV11.csv","csv")
#persist(StrctOp,"DISK_ONLY")
filter_StrctOp = head(StrctOp,n=999999)


##Below file has the list of column that wil be used for calculating the single consolidated weight and the the associated weight for each column
#ColWeight <- read.csv(file="ColumnWeightV9.csv", header=TRUE, sep=",")
f <- hdfs.file("hdfs://hadoop-master:54310/affinity/ColumnWeightV9.csv","r",buffersize=104857600)
m <- hdfs.read(f)
c <- rawToChar(m)
ColWeight <- read.table(textConnection(c),sep=",")

#filter_df_StrctOp <- head(select(filter_StrctOp, StrctOp$`_c1`, StrctOp$`_c3`),n=999999)

df_ColWeight <- createDataFrame(ColWeight)
filter_df_ColWeight <- head(df_ColWeight)

#levels_ColWeight <- factor(filter_df_ColWeight$V1)

###Extract all Unique block Ids in the file for splitting
UniqBlock<- unique(str_trim(filter_StrctOp[,c(1)]))

###########################
#Initialion for each block 
###########################

for (block in UniqBlock){
  #filter for specific block
  #StrctBlockOp_filter<-filter(filter_StrctOp, str_trim(filter_StrctOp[[1]]) == str_trim(eval(block)))#####################
  StrctBlockOp_filter<-subset(filter_StrctOp, str_trim(filter_StrctOp[[1]]) == str_trim(eval(block)))
  #StrctBlockOp_filter<-subset(filter_StrctOp, str_trim(filter_StrctOp[[1]]) == str_trim(eval("bronx")))
  
  #######################################
  ### Using aggregation formula for calculting single weight
  #######################################
  StrctBlockOp_filter$SingleWtCol<-0
  for (column in ColWeight$V1){
    #column<-'Rest_Score'
    colWt<-lookup(column,ColWeight[c(1,3)])
    StrctBlockOp_filter$SingleWtCol<- (as.numeric(StrctBlockOp_filter[[column]])^colWt) + StrctBlockOp_filter$SingleWtCol
    
  }
  ##Below DF is a new DF contaning only required primary keys and consildated lev score
  
  
  ReqColms<-StrctBlockOp_filter[,c(2,4)]
  ReqColms$SingleWtCol<-StrctBlockOp_filter$SingleWtCol*(-1)
  ReqColms.names  <- sort(unique(c(as.character(StrctBlockOp_filter[[2]]), as.character(StrctBlockOp_filter[[4]])))) ##Idenitify unique records
  
  ReqColms.dist <- matrix(-999, length(ReqColms.names), length(ReqColms.names)) ###Create  N*N matrix first with NULL
  dimnames(ReqColms.dist) <- list(ReqColms.names, ReqColms.names)
  ReqColms.ind <- rbind(cbind(match(ReqColms[[1]], ReqColms.names), match(ReqColms[[2]], ReqColms.names)),
                        cbind(match(ReqColms[[2]], ReqColms.names), match(ReqColms[[1]], ReqColms.names)))
  ReqColms.dist[ReqColms.ind] <- rep(ReqColms[[3]], 2)
  
  rowCount<-nrow(ReqColms.dist)
  ###Creation of a matrix and populating the distance for diagonal elements
  for(i in 1:(rowCount )){  # need an expression that retruns a sequence            
    for (j in 1:rowCount)   #                                                  
      if (i == j){          #                                                       
        ReqColms.dist[i,j] <- 0;   
      }
  }
  ###Final sparse matrix for each filtered block
  #ReqColms.dist
  
  #######################################################
  ##Affinity Propagation
  #######################################################
  ##############################################################
  ####Split into clusters 
  ##############################################################
  
  #ssim <- as.SparseSimilarityMatrix(sim, lower=-0.2)
  ReqColms.dist.sprse <- as.SparseSimilarityMatrix(ReqColms.dist, lower=-0.3)
  dimnames(ReqColms.dist.sprse) <- list(ReqColms.names, ReqColms.names)
  
  #Below is a better approach,Splits into clusters based on data
  #Rng<-preferenceRange(ReqColms.dist.sprse)# p to be fixed based on this
  #PValue<-quantile(Rng,prob=c(1))
  
  #apres1c<-apcluster(ReqColms.dist,p=-3.0,q=1, maxits=1000, convits=100)
  #apres1c<-apcluster(ReqColms.dist,p=PValue, maxits=1000, convits=100)
  #apres1c<-apcluster(ReqColms.dist.sprse,p=PValue,q=0, maxits=1000, convits=100)
  #apres1c
  #apres1c<-apcluster(ReqColms.dist.sprse, maxits=1000, convits=100)
  #preferenceRange(apres1c@sim,exact=TRUE)  
  apres1c<-apcluster(ReqColms.dist.sprse,q=0, maxits=1000, convits=100)
  
  # save as  ASCII file for each block
  sink("ClusterOutput.txt",append = TRUE)
  print("############################" )
  print("Processing Block details " )
  print(block)
  print(apres1c)
  sink()
  
}
sparkR.session.stop()