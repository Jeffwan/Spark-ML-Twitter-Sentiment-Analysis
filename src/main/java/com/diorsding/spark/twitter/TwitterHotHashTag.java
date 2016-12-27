package com.diorsding.spark.twitter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
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
 * Use JSON file to store twitter credentials
 * http://crunchify.com/how-to-read-json-object-from-file-in-java/
 *
 * @author jiashan
 *
 */
public class TwitterHotHashTag {

    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
        if (Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
            Logger.getRootLogger().setLevel(Level.WARN);
        }

        Helper.configureTwitterCredentials();
        SparkConf sparkConf = new SparkConf().setAppName("JavaTwitterHashTagJoinSentiments").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);

        // Concert twitter to POJO
        JavaDStream<Tweet> tweets =
                stream.map(twitter -> new Tweet(twitter.getUser().getName(), twitter.getText(), new Date()));

        hashTagAnalysis(stream);

        jssc.start();
        // 1.5.2 Doesn't need to throw InterruptedException here.
        jssc.awaitTermination();

    }

    public static void hashTagAnalysis(JavaReceiverInputDStream<Status> stream) {
        JavaDStream<String> splitLines = stream.flatMap(status -> Arrays.asList(status.getText().split(" ")));

        JavaDStream<String> filteredLines = splitLines.filter(line -> line.startsWith("#"));

        JavaPairDStream<String, Integer> hashTagsPairs =
                filteredLines.mapToPair(hashTag -> new Tuple2<String, Integer>(hashTag, 1));

        JavaPairDStream<String, Integer> hashTagPairsReduced =
                hashTagsPairs.reduceByKey((integer1, integer2) -> (integer1 + integer2));

        hashTagPairsReduced.cache();
        hashTagPairsReduced.print();

        // Find top count 60.
        // filteredLines.reduceByWindow(reduceFunc, windowDuration, slideDuration);

        // Find top count 5.
        filteredLines.foreachRDD(new Function<JavaRDD<String>, Void>() {
            @Override
            public Void call(JavaRDD<String> rddLines) throws Exception {
                rddLines.cache();

                List<String> top5 = rddLines.take(5);
                System.out.println(String.format("Popular topics in last 60 seconds (%d total) ", rddLines.count()));

                return null;
            }
        });

        filteredLines.foreachRDD(rddLines -> {
            rddLines.cache();

            List<String> top5 = rddLines.take(5);
            System.out.println(String.format("%s", top5));
            System.out.println(String.format("Popular topics in last 60 seconds (%d total) ", rddLines.count()));

            return null;
        });
    }
}
