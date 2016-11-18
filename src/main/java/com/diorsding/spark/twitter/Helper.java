package com.diorsding.spark.twitter;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;


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

    private static final String keyspace = "test";
    private static final String table = "twitter";

    public static String getKeyspace() {
        return keyspace;
    }

    public static String getTable() {
        return table;
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        Annotation annotation = pipeline.process("hello, welcome");

        List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
        for (CoreMap sentence : sentences) {
            String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
            System.out.println(sentiment + "\t" + sentence);
        }
    }

    public static void setSparkLogLevel(Level sparkLogLevel, Level streamingLogLevel) {
        Logger.getLogger("org.apache.spark").setLevel(sparkLogLevel);
        Logger.getLogger("org.apache.spark.streaming,NetworkInputTracker").setLevel(streamingLogLevel);
    }

    public static void configureTwitterCredentials() throws IOException, ParseException, FileNotFoundException {
        JSONParser parser = new JSONParser();

        Object object =
                parser.parse(new FileReader(TwitterHotHashTag.class.getClassLoader()
                        .getResource("twitter.json").getFile()));

        JSONObject jsonObject = (JSONObject) object;
        String consumerKey = (String) jsonObject.get("consumerKey");
        String consumerSecret = (String) jsonObject.get("consumerSecret");;
        String accessToken = (String) jsonObject.get("accessToken");;
        String accessTokenSecret = (String) jsonObject.get("accessTokenSecret");

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
    }

}
