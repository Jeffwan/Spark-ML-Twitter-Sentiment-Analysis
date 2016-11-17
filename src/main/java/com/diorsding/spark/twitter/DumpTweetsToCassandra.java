package com.diorsding.spark.twitter;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;


/**
 * Spark Cassandra Connector Example
 *
 * https://github.com/datastax/spark-cassandra-connector/blob/master/doc/7_java_api.md
 *
 * @author jiashan
 *
 */
public class DumpTweetsToCassandra {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<Status> inputStream = TwitterUtils.createStream(null);

        // Do some conversion to tweets
        JavaDStream<String> tweets = inputStream.flatMap(new FlatMapFunction<Status, String>() {
            @Override
            public Iterable<String> call(Status status) throws Exception {
                return Arrays.asList(status.getText().split(" "));
            }
        });

        JavaRDD<String> tweetsRDD = null;

        // Check later spark cassandra connector API
        // javaFunctions(tweetsRDD).writerBuilder("ks", "table", null).saveToCassandra();

        jssc.start();
        jssc.awaitTermination();
    }

}