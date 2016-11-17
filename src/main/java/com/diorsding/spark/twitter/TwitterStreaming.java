package com.diorsding.spark.twitter;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * http://bahir.apache.org/docs/spark/current/spark-streaming-twitter/
 *
 * Twitter Streaming Example
 * https://github.com/apache/bahir/blob/master/streaming-twitter/examples/src/main/java/org/apache
 * /spark/examples/streaming/twitter/JavaTwitterHashTagJoinSentiments.java
 *
 * 1. Solve Twitter Streaming 2.
 *
 * @author jiashan
 *
 */

public class TwitterStreaming {

    private static JavaStreamingContext jssc = null;
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String input) throws Exception {
                return Arrays.asList(input.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
