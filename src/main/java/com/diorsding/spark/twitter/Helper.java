package com.diorsding.spark.twitter;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


/**
 * https://github.com/amplab/training/blob/ampcamp4/streaming/java/TutorialHelper.java
 *
 * Twitter Streaming --> Spark Streaming (Preprocessing) --> Cassandra (Storage) Spark --> Cassnadra --> Spark SQL
 * Analytics --> MySQL (Process result) --> Virtualizaiton
 *
 *
 * @author jiashan
 *
 */
public class Helper {

    public static void setSparkLogLevel(Level sparkLogLevel, Level streamingLogLevel) {
        Logger.getLogger("org.apache.spark").setLevel(sparkLogLevel);
        Logger.getLogger("org.apache.spark.streaming.NetworkInputTracker").setLevel(streamingLogLevel);
    }

    public static void configureTwitterCredentials() throws IOException, ParseException, FileNotFoundException {
        JSONParser parser = new JSONParser();

        Object object =
                parser.parse(new FileReader(TwitterHotHashTag.class.getClassLoader()
                        .getResource("twitter.json").getFile()));

        JSONObject jsonObject = (JSONObject) object;
        String consumerKey = jsonObject.get("consumerKey").toString();
        String consumerSecret = jsonObject.get("consumerSecret").toString();
        String accessToken = jsonObject.get("accessToken").toString();
        String accessTokenSecret = jsonObject.get("accessTokenSecret").toString();

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
    }

}
