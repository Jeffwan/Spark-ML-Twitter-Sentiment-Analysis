package com.diorsding.spark.twitter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.json.simple.parser.ParseException;

import twitter4j.Status;

public class TwitterNLP {

    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
        Helper.setSparkLogLevel(Level.WARN, Level.WARN);

        SparkConf sparkConf = new SparkConf().setAppName("TwitterNLP").setMaster("local[2]");

        String[] filters = Helper.configureTwitterCredentials();
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, filters);



        JavaDStream<String> tweets = stream.map(new Function<Status, String>() {
            @Override
            public String call(Status status) throws Exception {
                return status.getText();
            }
        });

        JavaDStream<String> tweetWithScoreDStream = tweets.map(new Function<String, String>() {
            @Override
            public String call(String tweetText) throws Exception {
                return SentimentAnalysisUtils.detectSentiment(tweetText);
            }
        });

        // Save results to Mysql. DStream -> List<RDD> -> List<String> / per RDD.
        tweetWithScoreDStream.foreachRDD(new Function<JavaRDD<String>, Void>() {

            @Override
            public Void call(JavaRDD<String> tweetStreamRDD) throws Exception {
                Connection connection = DriverManager.getConnection("jdbc://localhost:3306/test", "root", "root");
                // For each RDD. for

                // "INSEERT INTO SENTIMENT(SENTIMENT) VALUES (result)";

                tweetStreamRDD.foreach(new VoidFunction<String>() {

                    @Override
                    public void call(String record) throws Exception {
                        // save record here.
                    }
                });

                return null;
            }


        });


        jssc.start();

        // 1.5.2 Doesn't need to throw InterruptedException here.
        jssc.awaitTermination();
    }

}
