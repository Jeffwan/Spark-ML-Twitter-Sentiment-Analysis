package com.diorsding.spark.twitter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.json.simple.parser.ParseException;

import scala.Tuple2;
import twitter4j.Status;

/**
 * http://bahir.apache.org/docs/spark/current/spark-streaming-twitter/
 *
 * Twitter Streaming Example
 * https://github.com/apache/bahir/blob/master/streaming-twitter/examples/src/main/java/org/apache
 * /spark/examples/streaming/twitter/JavaTwitterHashTagJoinSentiments.java
 *
 * 1. Solve Twitter Streaming
 * 2.
 *
 * @author jiashan
 *
 */

public class TwitterStreaming {

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException,
            ParseException {

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        // if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
        // Logger.getRootLogger().setLevel(Level.WARN);
        // }

        // wordCountDemo();

        streamingDemo();

    }

    public static void streamingDemo() throws FileNotFoundException, IOException, ParseException {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("TwitterStreaming");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        String[] filters = Helper.configureTwitterCredentials();
        // No need to fill filters in.
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, new String[] {});

        // Concert twitter to POJO

        JavaDStream<Status> tweetsWithGeo = stream.filter(status -> status.getText().contains("#"));

        JavaDStream<Tweet> tweets =
                tweetsWithGeo.map(twitter -> new Tweet(twitter.getUser().getName(), twitter.getText(), new Date()));

        tweets.print();

        jssc.start();
        jssc.awaitTermination();
    }

    public static void wordCountDemo() {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(word -> Arrays.asList(word.split(" ")));
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> (i1 + i2));

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
