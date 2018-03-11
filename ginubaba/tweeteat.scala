import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._

import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.SomeColumns

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.SomeColumns


object TweatEat  extends App {
    val batchInterval_s = 1
    val totalRuntime_s = 32
	
    //add your creds below
    System.setProperty("twitter4j.oauth.consumerKey", "ccwtl2crmKH0p30pHocmcvouh")
    System.setProperty("twitter4j.oauth.consumerSecret", "gMjli4yVo2Bf3kM3ejHez1vDtB5AqK7r2HTqk9pvx9GNEF7F9n")
    System.setProperty("twitter4j.oauth.accessToken", "1414214346-SIvjGchjh09A6r3YIZzTNSaj0LOBF3kqJL6syrW")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "2EHxskNPxczHxsjmrqiA1C1peydSjNxf9kHiwPqwSUqZ0")
	
    // create SparkConf
    // val conf = new SparkConf().setAppName("mids tweeteat");
	// val conf = new SparkConf().setAppName("mids tweeteat").set("spark.cassandra.connection.host", "127.0.0.1");
	val conf = new SparkConf().setAppName("mids tweeteat").set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.connections_per_executor_max","2");

    // batch interval determines how often Spark creates an RDD out of incoming data
    val ssc = new StreamingContext(conf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None)

    // extract desired data from each status during sample period as class "TweetData", store collection of those in new RDD
    // val tweetData = stream.map(status => TweetData(status.getId, status.getUser.getScreenName, status.getText.trim))
    // tweetData.foreachRDD(rdd => {
		// data aggregated in the driver
		// println(s"A sample of tweets I gathered over ${batchInterval_s}s: ${rdd.take(10).mkString(" ")} (total tweets fetched: ${rdd.count()})")
    // })
    stream.map(status => TweetData(status.getId, status.getUser.getScreenName, status.getText.trim)).saveToCassandra("streaming", "tweetdata", SomeColumns("id", "author", "tweet"))
    
    // start consuming stream
    ssc.start
    ssc.awaitTerminationOrTimeout(totalRuntime_s * 1000)
    ssc.stop(true, true)

    println(s"============ Exiting ================")
    System.exit(0)
}

case class TweetData(id: Long, author: String, tweet: String)
