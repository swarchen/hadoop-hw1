# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/WordCount/Output/
#hadoop jar WordCount.jar wordcount.WordCount /user/shared/WordCount/Input /user/TA/WordCount/Output
#hdfs dfs -cat /user/TA/WordCount/Output/part-*

output_dir=/user/s104062599/WordCount/output
hdfs dfs -rm -r $output_dir
hadoop jar WordCount.jar wordcount.WordCount /user/s104062599/WordCount/input $output_dir
hdfs dfs -cat $output_dir/part-*
