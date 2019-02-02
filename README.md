# SharedBicycleAnalysis
Shared bike log analysis is divided into offline and real-time parts.  
The offline part:  
flume collected the log into HDFS, sparkCore and sparkSql read and analyzed the data from HDFS, and saved the generated report data into mysql.SparkCore processes the log to generate the user's tag information, and then generates the business circle tag by calling Baidu Web Api to encode the longitude and latitude backward.Store detailed data of label information in Hbase and iterated data in ElasticSearch for label retrieval.  
Real-time part:  
the Taildir Source of flume was used to gather data into kafka 1.0 cluster, and SparkStreaming was used to read data by dc mode, and the processed results were put into Redis in batches of five seconds  
