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
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.json.simple.parser.ParseException;

import scala.Tuple2;
import twitter4j.Status;


/**
 *
 * @author jiashan
 *
 */
public class TwitterHotHashTag extends TwitterSparkBase {

    private static final Long BATCH_INTERVAL = 2000l;
    private static final Long WINDOW_DURATION = 20000l;
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

        JavaDStream<String> filteredLines = splitLines.filter(line -> line.startsWith("#"));

        JavaPairDStream<String, Integer> hashTagsPairs =
                filteredLines.mapToPair(hashTag -> new Tuple2<String, Integer>(hashTag, 1));

        JavaPairDStream<String, Integer> hashTagPairsReduced =
                hashTagsPairs.reduceByKeyAndWindow((integer1, integer2) -> (integer1 + integer2),
                    new Duration(WINDOW_DURATION), new Duration(SLIDE_DURATION));

        hashTagPairsReduced.print();

        // How to automatically sort these tags and

//        hashTagPairsReduced.foreach(new Function<JavaPairRDD<String, Integer>, Void>() {
//            @Override
//            public Void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
//                List<Tuple2<String, Integer>> collect = stringIntegerJavaPairRDD.collect();
//                System.out.println(collect.size());
//                System.out.println(collect);
//
//                return null;
//            }
//        });

//        // Find top count 5.
//        filteredLines.foreachRDD(new Function<JavaRDD<String>, Void>() {
//            @Override
//            public Void call(JavaRDD<String> rddLines) throws Exception {
//                List<String> top5 = rddLines.take(5);
//                System.out.println(String.format("Popular topics in last 60 seconds (%d total) ", rddLines.count()));
//
//                return null;
//            }
//        });


//        filteredLines.foreachRDD(rddLines -> {
//            rddLines.cache();
//
//            List<String> top5 = rddLines.take(5);
//            System.out.println(String.format("%s", top5));
//            System.out.println(String.format("Popular topics in last 10 seconds (%d total) ", rddLines.count()));
//
//            return null;
//        });

    }
}
