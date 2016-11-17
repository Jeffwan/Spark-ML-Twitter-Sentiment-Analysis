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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
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

    class Tweet {
        Date date;
        String user;
        String text;

        public Tweet(Date date, String user, String text) {
            super();
            this.date = date;
            this.user = user;
            this.text = text;
        }
    }

    @SuppressWarnings("serial")
    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {

        if (Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
            Logger.getRootLogger().setLevel(Level.WARN);
        }

        String[] filters = Helper.configureTwitterCredentials();
        SparkConf sparkConf = new SparkConf().setAppName("JavaTwitterHashTagJoinSentiments").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, filters);

        // TODO? how to concert string to POJO?
        JavaDStream<Tweet> tweets = stream.map(new Function<Status, Tweet>() {
            @Override
            public Tweet call(Status status) throws Exception {
                Date nowTime = new Date();
                String user = status.getUser().getName();
                String text = status.getText();
                // new Tweet(nowTime, user, text)
                return null;
            }
        });

        hashTagAnalysis(stream);

        jssc.start();

        // 1.5.2 Doesn't need to throw InterruptedException here.
        jssc.awaitTermination();

    }

    public static void hashTagAnalysis(JavaReceiverInputDStream<Status> stream) {
        JavaDStream<String> splitLines = stream.flatMap(new FlatMapFunction<Status, String>() {
            @Override
            public Iterable<String> call(Status status) throws Exception {
                // TODO Auto-generated method stub
                return Arrays.asList(status.getText().split(" "));
            }
        });

        JavaDStream<String> filteredLines = splitLines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                return line.startsWith("#");
            }
        });

        JavaPairDStream<String, Integer> hashTagsPairs =
                filteredLines.mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String hashTag) throws Exception {
                        return new Tuple2<String, Integer>(hashTag, 1);
                    }
                });


        JavaPairDStream<String, Integer> hashTagPairsReduced =
                hashTagsPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer1, Integer integer2) throws Exception {
                        return integer1 + integer2;
                    }
                });

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
    }




}
