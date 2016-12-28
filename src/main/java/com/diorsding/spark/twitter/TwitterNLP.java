package com.diorsding.spark.twitter;

import com.diorsding.spark.utils.LogUtils;
import com.diorsding.spark.utils.OAuthUtils;
import com.diorsding.spark.utils.SentimentUtils;

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
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.json.simple.parser.ParseException;

import twitter4j.Status;

/**
 * https://github.com/vspiewak/twitter-sentiment-analysis/blob/master/src/main/scala/com/github/vspiewak/util/SentimentAnalysisUtils.scala
 * https://devpost.com/software/spark-mllib-twitter-sentiment-analysis
 * https://github.com/P7h/Spark-MLlib-Twitter-Sentiment-Analysis/wiki
 *
 *
 * I am not sure if I need to store these analysis data into mysql
 * @author jiashan
 *
 */
public class TwitterNLP {

    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
        LogUtils.setSparkLogLevel(Level.WARN, Level.WARN);
        OAuthUtils.configureTwitterCredentials();

        SparkConf sparkConf = new SparkConf().setAppName(TwitterNLP.class.getSimpleName()).setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);

        JavaDStream<String> tweets = stream.map(status -> status.getText());

        JavaDStream<String> tweetWithScoreDStream =
                tweets.map(tweetText -> SentimentUtils.calculateSentimentScore(tweetText));

        // Save results to Mysql. DStream -> List<RDD> -> List<String> / per RDD.
        // Seems not connector like cassandra. Need to use JDBC
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
