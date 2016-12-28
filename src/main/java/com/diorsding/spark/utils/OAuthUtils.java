package com.diorsding.spark.utils;

import com.diorsding.spark.twitter.TwitterHotHashTag;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by jiashan on 12/27/16.
 */
public class OAuthUtils {

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
