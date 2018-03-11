// scalastyle:off println
import org.apache.log4j.{Level, Logger}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._


object Main extends App {
    
    if (args.length < 5) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> <top n count> [<filters>]")
      System.exit(1)
    }
    val smallTime = 120
    val bigTime = 1800
    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret, nStr) = args.take(5)
    val filters = args.takeRight(args.length - 5)
    val n = nStr.toInt

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    //Get (hash tags, users)
    val tagsAndUsers= stream.map(status => (status.getText.split(" ").filter(_.startsWith("#")), status.getText.split(" ").filter(_.startsWith("@")).mkString(" ").concat(" $").concat(status.getUser().getScreenName()))).flatMap{ case (k,v) => k.map(kx => (kx, v))}

    //Count number of times hash tag is used, concatenate users for small time interval
    val topTagUserCountsSmall = tagsAndUsers.map{ case (topic, users) => (topic, (users, 1))}.reduceByKeyAndWindow((x, y) => (x._1.concat(" ").concat(y._1), x._2 + y._2), Seconds(smallTime))
                     .map{case (topic, (users, count)) => (count, (topic, users))}
                     .transform(_.sortByKey(false))
    
    //Run and print for the small time interval
    topTagUserCountsSmall.foreachRDD(rdd => {
      val topList = rdd.take(n)
      println("\n\nPopular hashtags over the last %s seconds (%s total tags):".format(smallTime, rdd.count()))
      println("HashTags\tCount\t@Users:")
      topList.foreach{case (count, (tag, users)) => println("%s\t%s\t%s)".format(tag, count, users.split(" ").toSet))}
    })

    //Count number of times hash tag is used, concatenate users for big time interval
    val topTagUserCountsBig = tagsAndUsers.map{ case (topic, users) => (topic, (users, 1))}.reduceByKeyAndWindow((x, y) => (x._1.concat(" ").concat(y._1), x._2 + y._2), Seconds(bigTime))
                     .map{case (topic, (users, count)) => (count, (topic, users))}
                     .transform(_.sortByKey(false))

    //Run and print for the big time interval
    topTagUserCountsBig.foreachRDD(rdd => {
      val topList = rdd.take(n)
      println("\n\nPopular hashtags over the last %s seconds (%s total tags):".format(bigTime, rdd.count()))
      println("HashTags\tCount\t@Users,$Authors:")
      topList.foreach{case (count, (tag, users)) => println("%s\t%s\t%s)".format(tag, count, users.split(" ").toSet.mkString(", ")))}
    })


    ssc.start()
    ssc.awaitTermination()
}
