package com.diorsding.spark.twitter;

/**
 * Created by jiashan on 12/27/16.
 */
public class Constants {

    public static final String CASSANDRA_TWITTER_KEYSPACE = "test";
    public static final String CASSANDRA_TWITTER_TABLE = "tweet";

    public static final String CASSANDRA_CONNECTION_HOST_KEY = "spark.cassandra.connection.host";
    public static final String CASSANDRA_CONNECTION_HOST_VALUE = "10.148.254.9";

    public static final String REDIS_CONNECTION_HOST = "10.148.254.9";
    public static final int REDIS_CONNECTION_PORT = 6379;

    public static final String REDIS_CHANNEL_STREAMING_TWEET = "StreamingTweet";
    public static final String REDIS_CHANNEL_HOT_HASHTAG = "HotHashTag";

}
