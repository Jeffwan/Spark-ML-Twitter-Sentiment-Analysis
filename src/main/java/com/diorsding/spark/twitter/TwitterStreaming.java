package com.diorsding.spark.twitter;

import com.diorsding.spark.utils.CassandraUtils;
import com.diorsding.spark.utils.SentimentUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.json.simple.parser.ParseException;

import twitter4j.Status;

import scala.Tuple2;

/**
 * http://bahir.apache.org/docs/spark/current/spark-streaming-twitter/
 *
 * Twitter Streaming Example
 * https://github.com/apache/bahir/blob/master/streaming-twitter/examples/src/main/java/org/apache
 * /spark/examples/streaming/twitter/JavaTwitterHashTagJoinSentiments.java
 *
 * 1. Solve Twitter Streaming
 *
 *
 * Spark Cassandra Connector:
 * https://github.com/datastax/spark-cassandra-connector/blob/master/doc/7_java_api.md
 *
 * https://github.com/amplab/training/blob/ampcamp4/streaming/java/TutorialHelper.java
 *
 * Twitter Streaming --> Spark Streaming (Preprocessing) --> Cassandra (Storage) Spark --> Cassnadra --> Spark SQL
 * Analytics --> MySQL (Process result) --> Virtualizaiton
 *
 *
 * @author jiashan
 *
 *
 */

public class TwitterStreaming extends TwitterSparkBase {

    public static void main(String[] args) throws InterruptedException, IOException, ParseException {
        preSetup();


        SparkConf sparkConf = new SparkConf().setMaster("local[2]")
            .setAppName(TwitterStreaming.class.getSimpleName())
            .set(Constants.CASSANDRA_CONNECTION_HOST_KEY, Constants.CASSANDRA_CONNECTION_HOST_VALUE);

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);

        // Different flows
        processRealTimeGeo(stream);
        processHotHashTag(stream);

        jssc.start();
        jssc.awaitTermination();
    }

    private static void processHotHashTag(JavaReceiverInputDStream<Status> stream) {
        JavaDStream<String> hashTagRDD = stream
            .filter(tweet -> isTweetEnglish(tweet))
            .flatMap(status -> Arrays.asList(status.getText().split(" ")))
            .filter(line -> line.startsWith("#"))
            .map(line -> line.trim());

        JavaPairDStream<Integer, String> topicCounts60RDD = hashTagRDD
            .mapToPair(hashTag -> new Tuple2<>(hashTag, 1))
            .reduceByKeyAndWindow((integer1, integer2) -> (integer1 + integer2), Durations.seconds(3600))
            .mapToPair(tuple -> tuple.swap())
            .transformToPair(integerStringJavaPairRDD -> integerStringJavaPairRDD.sortByKey(false));

        topicCounts60RDD.foreachRDD(new Function<JavaPairRDD<Integer, String>, Void>() {
            @Override
            public Void call(JavaPairRDD<Integer, String> topicCounts60RDD) throws Exception {
                List<Tuple2<Integer, String>> top10Topics = topicCounts60RDD.take(10);// get Top 10.

                top10Topics.forEach(tuple -> System.out.println(String.format("%s, (%d tweets)", tuple._2(), tuple._1())));
                // Cache somewhere and show UI
                return null;
            }
        });
    }


    public static void processRealTimeGeo(JavaReceiverInputDStream<Status> stream) {
        // Filter tweets with geoLocation
        JavaDStream<Tweet> tweets = stream
            .filter(status -> hasGeoLocation(status) && isTweetEnglish(status))
            .map(status -> buildNewTweet(status));

        CassandraUtils.dumpTweetsToCassandra(tweets);
    }

    private static Tweet buildNewTweet(Status status) {
        return new Tweet(status.getUser().getId(),
            status.getUser().getName(),
            status.getUser().getScreenName(),
            status.getUser().getMiniProfileImageURL(),
            replaceNewLines(status.getText()),
            status.getGeoLocation() == null ? null : status.getGeoLocation().getLatitude(),
            status.getGeoLocation() == null ? null : status.getGeoLocation().getLongitude(),
            status.getLang(),
            status.getSource(),
            SentimentUtils.calculateWeightedSentimentScore(status.getText()),
            new Date());
    }

    // Override base class function
    protected static boolean isTweetEnglish(Status status) {
//        return "en".equals(status.getLang()) && "en".equals(status.getUser().getLang());
        return true;
    }

    private static boolean hasGeoLocation(Status status) {
        return status.getGeoLocation() != null;
    }

    private static String replaceNewLines(String text) {
        return text.replace("\n", "");
    }
}
