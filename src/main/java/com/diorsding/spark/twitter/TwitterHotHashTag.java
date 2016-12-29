package com.diorsding.spark.twitter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.Stat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.json.simple.parser.ParseException;

import scala.Tuple2;
import twitter4j.Status;


/**
 * This is a e2e test class. Main logic will be integrated in to {@link TwitterStreaming}
 *
 * @author jiashan
 *
 */
public class TwitterHotHashTag extends TwitterSparkBase {

    private static final Long BATCH_INTERVAL = 2000l;
    private static final Long WINDOW_DURATION = 10000l;
    private static final Long SLIDE_DURATION = 2000l;

    public static void main(String[] args) throws IOException, ParseException {
        preSetup();

        hotHashTagAnalyzer();
    }

    private static void hotHashTagAnalyzer() {
        SparkConf sparkConf = new SparkConf()
            .setAppName(TwitterHotHashTag.class.getSimpleName())
            .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(BATCH_INTERVAL)); // Batch Interval

        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);

        hashTagAnalysis(stream);

        jssc.start();

        // 1.5.2 Doesn't need to throw InterruptedException here.
        jssc.awaitTermination();
    }


    public static void hashTagAnalysis(JavaReceiverInputDStream<Status> stream) {
        JavaDStream<Status> enTweets = stream.filter(tweet -> isTweetEnglish(tweet));
        JavaDStream<String> splitLines = enTweets.flatMap(status -> Arrays.asList(status.getText().split(" ")));
        JavaDStream<String> filteredLines = splitLines.filter(line -> line.startsWith("#")).map(line -> line.trim());


        JavaPairDStream<String, Integer> hashTagsPairs =
                filteredLines.mapToPair(hashTag -> new Tuple2<String, Integer>(hashTag, 1));

        /* Did not use SLIDE_DURATION
         *
        JavaPairDStream<String, Integer> hashTagPairsReduced =
                hashTagsPairs.reduceByKeyAndWindow((integer1, integer2) -> (integer1 + integer2),
                    new Duration(WINDOW_DURATION), new Duration(SLIDE_DURATION));

        hashTagPairsReduced.print();
        */

        JavaPairDStream<Integer, String> topicCounts60 = hashTagsPairs
            .reduceByKeyAndWindow((integer1, integer2) -> (integer1 + integer2), Durations.seconds(3600))
            .mapToPair(tuple -> tuple.swap())
            .transformToPair(integerStringJavaPairRDD -> integerStringJavaPairRDD.sortByKey(false));

        topicCounts60.foreachRDD(new Function<JavaPairRDD<Integer, String>, Void>() {
            @Override
            public Void call(JavaPairRDD<Integer, String> topicCounts60RDD) throws Exception {
                List<Tuple2<Integer, String>> top10Topics = topicCounts60RDD.take(10);// get Top 10.

                top10Topics.forEach(tuple -> System.out.println(String.format("%s, (%d tweets)", tuple._2(), tuple._1())));
                // Cache somewhere and show UI
                return null;
            }
        });
    }
}
