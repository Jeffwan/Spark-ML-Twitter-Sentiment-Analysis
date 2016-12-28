package com.diorsding.spark.twitter;

import com.diorsding.spark.utils.CassandraUtils;
import com.diorsding.spark.utils.SentimentUtils;

import java.io.IOException;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.json.simple.parser.ParseException;

import twitter4j.Status;

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

        streamingEntry();
    }

    public static void streamingEntry() {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]")
            .setAppName(TwitterStreaming.class.getSimpleName())
            .set(Constants.CASSANDRA_CONNECTION_HOST_KEY, Constants.CASSANDRA_CONNECTION_HOST_VALUE);

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);

        // Filter tweets with geoLocation
        JavaDStream<Status> enGeoTweets = stream.filter(status -> hasGeoLocation(status) && isTweetEnglish(status));

        // Preprocess tweets
        // TODO: Use AVRO data instead of my own data. Store more information.
        JavaDStream<Tweet> tweets = enGeoTweets.map(status -> buildNewTweet(status));

        CassandraUtils.dumpTweetsToCassandra(tweets);

        jssc.start();
        jssc.awaitTermination();
    }

    private static Tweet buildNewTweet(Status status) {
        return new Tweet(status.getUser().getId(),
            status.getUser().getName(),
            status.getUser().getScreenName(),
            status.getUser().getMiniProfileImageURL(),
            replaceNewLines(status.getText()),
            status.getGeoLocation() == null ? null : status.getGeoLocation().getLatitude(),
            status.getGeoLocation() == null ? null : status.getGeoLocation().getLongitude(),
            SentimentUtils.calculateWeightedSentimentScore(status.getText()),
            new Date());
    }

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
