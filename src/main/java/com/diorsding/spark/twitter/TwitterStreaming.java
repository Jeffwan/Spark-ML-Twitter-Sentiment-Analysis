package com.diorsding.spark.twitter;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;

import java.io.IOException;
import java.util.Date;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
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
 * @author jiashan
 *
 */

public class TwitterStreaming {
    public static void main(String[] args) throws InterruptedException, IOException, ParseException {
        preSetup();

        streamingDemo();
    }

    private static void preSetup() throws IOException, ParseException {
        Helper.setSparkLogLevel(Level.WARN, Level.WARN);

        Helper.configureTwitterCredentials();
    }

    public static void streamingDemo() {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]")
            .setAppName(TwitterStreaming.class.getSimpleName())
            .set(Constants.CASSANDRA_CONNECTION_HOST_KEY, Constants.CASSANDRA_CONNECTION_HOST_VALUE);
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        // No need to fill filters in.
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);

        // Concert twitter to POJO
        JavaDStream<Status> tweetsWithTag = stream.filter(status -> status.getText().contains("#"));

        // TODO: Use AVRO data instead of my own data. Store more information.
//        JavaDStream<Tweet> tweets =
//                tweetsWithTag.map(twitter -> new Tweet(twitter.getUser().getName(), twitter.getText(), new Date()));

        JavaDStream<Tweet> tweets = tweetsWithTag.map(new Function<Status, Tweet>() {
            @Override
            public Tweet call(Status status) throws Exception {
                System.out.println(status.toString());

                return new Tweet(status.getUser().getName(), status.getText(), new Date());
            }
        });

//        tweets.print();

        /*
         * Write data to Cassandra. Since what we get is JavaDStream from Streaming, we use CassandraStreamingJavaUtil
         * instead. Do not use RDD here. It make problem complex because we need to convert DStream to RDD again.
         *
         */
        CassandraStreamingJavaUtil.javaFunctions(tweets)
            .writerBuilder(Constants.CASSANDRA_TWITTER_KEYSPACE, Constants.CASSANDRA_TWITTER_TABLE, CassandraJavaUtil.mapToRow(Tweet.class))
            .saveToCassandra();

        jssc.start();
        jssc.awaitTermination();
    }
}
