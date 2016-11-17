package com.diorsding.spark.twitter;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import twitter4j.Status;


/**
 *
 *
 * @author jiashan
 *
 */
public class JavaTwitterHashTagJoinSentiments {


    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
        // if (args.length < 4) {
        // System.err.println("Usage: JavaTwitterHashTagJoinSentiments"
        // + " <ConsumerKey> <ConsumerSecret> <AccessToken> <AccessTokenSecret> [<filters>]");
        //
        // System.exit(1);
        // }

        if (Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
            Logger.getRootLogger().setLevel(Level.WARN);
        }


        JSONParser parser = new JSONParser();
        Object object = parser.parse(new FileReader("twitter.json"));

        JSONObject jsonObject = (JSONObject) object;
        String consumerKey = (String) jsonObject.get("consumerKey");
        String consumerSecret = (String) jsonObject.get("consumerSecret");;
        String accessToken = (String) jsonObject.get("accessToken");;
        String accessTokenSecret = (String) jsonObject.get("accessTokenSecret");

        System.out.println(consumerKey);
        System.out.println(consumerSecret);
        System.out.println(accessToken);
        System.out.println(accessTokenSecret);


        // String[] filters = Arrays.copyOfRange(args, 4, args.length);

        String[] filters = new String[] {consumerKey, consumerSecret, accessToken, accessTokenSecret};

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        SparkConf sparkConf = new SparkConf().setAppName("JavaTwitterHashTagJoinSentiments").setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, filters);

        stream.print();

        JavaDStream<String> words = stream.flatMap(new FlatMapFunction<Status, String>() {
            public Iterable<String> call(Status status) throws Exception {

                return Arrays.asList(status.getText().split(" "));
            }
        });

        words.print();
        JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
            public Boolean call(String word) throws Exception {
                return word.startsWith("#");
            }
        });

        hashTags.print();

        jssc.start();

        jssc.awaitTermination();

    }
}
