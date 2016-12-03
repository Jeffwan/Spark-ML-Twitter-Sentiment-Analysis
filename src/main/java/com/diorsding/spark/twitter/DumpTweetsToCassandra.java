package com.diorsding.spark.twitter;

import java.io.FileNotFoundException;
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

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;


/**
 * Spark Cassandra Connector Example
 *
 * https://github.com/datastax/spark-cassandra-connector/blob/master/doc/7_java_api.md
 *
 * @author jiashan
 *
 */
public class DumpTweetsToCassandra {

    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("NetworkWordCount")
                .set("spark.cassandra.connection.host", "10.148.254.9");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        Helper.configureTwitterCredentials();
        JavaReceiverInputDStream<Status> inputStream = TwitterUtils.createStream(jssc, new String[] {});

        // Convert Streaming data to Tweet POJOs
        JavaDStream<Tweet> tweets =
                inputStream.map(status -> new Tweet(status.getUser().getName(), status.getText(), new Date()));

        /**
         * TODO: Validate JavaDStream content? If not valid streaming data returns, we'd better skip hitting cassandra?
         *
         * Consider ETL process for twitter. I think maybe we can define some columns and store in cassandra.
         */

        /*
         * Write data to Cassandra. Since what we get is JavaDStream from Streaming, we use CassandraStreamingJavaUtil
         * instead. Do not use RDD here. It make problem complex because we need to convert DStream to RDD again.
         * 
         * It's hard to store streaming data to file..? If we can aggregate file, then we can have another snapshot in
         * HDFS as file.
         */
        CassandraStreamingJavaUtil.javaFunctions(tweets)
                .writerBuilder(Helper.getKeyspace(), Helper.getTable(), CassandraJavaUtil.mapToRow(Tweet.class))
                .saveToCassandra();


        jssc.start();
        jssc.awaitTermination();
    }

}
