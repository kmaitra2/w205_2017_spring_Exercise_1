sbt package && $SPARK_HOME/bin/spark-submit   --packages org.apache.spark:spark-streaming_2.11:2.1.0 --packages org.apache.bahir:spark-streaming-twitter_2.11:2.1.0   --master spark://spark1:7077 $(find target -iname "*.jar")   $CONKEY $CONSECRET $ACCESSTOKEN $ACCESSSECRET 10 > output.txt
