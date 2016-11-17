package com.diorsding.spark.twitter;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

public class TwitterPopularTags {

    private static JavaStreamingContext jssc = null;

    public static void main(String[] args) {
        if (args.length < 6) {
            System.out.println("Usage: miss parameters");
            System.exit(1);
        }

        List<String> configurations = new ArrayList<String>();

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkHome");
        jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaDStream<Status> tweets = TwitterUtils.createStream(jssc);



    }

}
