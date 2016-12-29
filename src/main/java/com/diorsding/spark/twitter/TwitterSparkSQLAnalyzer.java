package com.diorsding.spark.twitter;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.diorsding.spark.utils.LogUtils;
import com.diorsding.spark.utils.SentimentUtils;

import twitter4j.Status;

import java.util.Date;

/**
 * Read Spark Cassandra Connector Doc Carefully. All are needed.
 * https://github.com/datastax/spark-cassandra-connector/tree/master/doc
 *
 * @author jiashan
 *
 */

public class TwitterSparkSQLAnalyzer {

    private static final String TWEET_CONTENT_FIELD = "text";
    private static final String TWEET_DATAFRAME_TABLE = "tweetTable";

    public static void main(String[] args) {
        LogUtils.setSparkLogLevel(Level.INFO, Level.INFO);

        sparkSQLAnalyzer();
    }

    private static void sparkSQLAnalyzer() {
        SparkConf sparkConf =
                new SparkConf().setMaster("local[2]")
                    .setAppName(TwitterSparkSQLAnalyzer.class.getSimpleName())
                    .set(Constants.CASSANDRA_CONNECTION_HOST_KEY, Constants.CASSANDRA_CONNECTION_HOST_VALUE);

        SparkContext sc = new SparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // Step 1. Convert Table to RDD and cache.
        readTweetTable(sc, sqlContext);

        // Step 2. SparkSQL Analysis
        getTweetsSample(sqlContext);

        findActiveLanguages(sqlContext);

        findMostCommonDevice(sqlContext);

        // TODO: findActiveTimeWindow(sqlContext);

        // TODO: find influenced user

        sc.stop();
    }


    private static void readTweetTable(SparkContext sc, SQLContext sqlContext) {
        /** Get CassandraRow
        CassandraTableScanJavaRDD<CassandraRow> data =
                CassandraJavaUtil.javaFunctions(sc).cassandraTable(Constants.CASSANDRA_TWITTER_KEYSPACE, Constants.CASSANDRA_TWITTER_TABLE);
        */

        CassandraTableScanJavaRDD<Tweet> data = CassandraJavaUtil.javaFunctions(sc)
            .cassandraTable(Constants.CASSANDRA_TWITTER_KEYSPACE, Constants.CASSANDRA_TWITTER_TABLE,
            CassandraJavaUtil.mapRowTo(Tweet.class));

        // Filter non-empty tweets
        JavaRDD<Tweet> nonEmptyTweetsRDD = data.filter(cassandraRow -> !cassandraRow.getText().trim().isEmpty());

        /* Not sure how to convert CassandraRow to DF */
        // JavaRDD<String> jsonFormatTweetsRDD = nonEmptyTweetsRDD.map(cassandraRow -> cassandraRow.toString());

        /**
         * Date is not supported in SparkSQL now. http://stackoverflow.com/questions/29731011/sparksql-not-supporting-java-util-date
         *
         * By the way, RDD is immutable. We can not edit it in VoidFunction but have to convert to a new Tweet object.
         */
        JavaRDD<Tweet> nonDateTweetsRDD = nonEmptyTweetsRDD.map(tweet -> buildNewTweet(tweet));

        DataFrame tweetTable = sqlContext.createDataFrame(nonDateTweetsRDD, Tweet.class);

        tweetTable.registerTempTable(TWEET_DATAFRAME_TABLE);

        sqlContext.cacheTable(TWEET_DATAFRAME_TABLE);

        // Let's check table schema
        tweetTable.printSchema();
    }

    private static Tweet buildNewTweet(Tweet tweet) {
        return new Tweet(tweet.getId(),
            tweet.getUser(),
            tweet.getScreenName(),
            tweet.getProfileImageUrl(),
            tweet.getText(),
            tweet.getLatitude(),
            tweet.getLongitude(),
            tweet.getLanguage(),
            tweet.getDevice(),
            tweet.getScore(),
            null);
    }

    /**
     * The following format is for CassandraRow. It's not helpful for DF analysis.
     * +--------------------+
     * |     _corrupt_record|
     * +--------------------+
     * |CassandraRow{date...|
     * |CassandraRow{date...|
     * |CassandraRow{date...|
     *
     * CassandraRow{date: 2016-12-27 17:22:52-0800, user: りょう＠複ワイド至上主義, id: 306043004, latitude: 35.630029, longitude: 139.7938556,
     * profile_image_url: http://pbs.twimg.com/profile_images/616608945721839617/oWP3C0_S_mini.jpg, score: -1, screen_name: xRulershipx,
     * text: 有明着いた！}
     *
     * @param sqlContext
     */
    private static void getTweetsSample(SQLContext sqlContext) {
        DataFrame tweetsDF = sqlContext.sql("select * from tweetTable Limit 10");

        tweetsDF.show();
    }

    private static void findActiveLanguages(SQLContext sqlContext) {
        // More active languages
        DataFrame activeUserDF =
                sqlContext.sql("select language, count(*) as cnt from tweetTable"
                        + " group by language order by cnt desc limit 25 ");
        Row[] activeUserRows = activeUserDF.collect();

        System.out.println("find Active Languages");
        for (Row row : activeUserRows) {
            System.out.println(row);
        }
    }

    private static void findMostCommonDevice(SQLContext sqlContext) {
        // More active languages
        DataFrame activeUserDF =
            sqlContext.sql("select device, count(*) as cnt from tweetTable"
                + " group by device order by cnt desc limit 25 ");
        Row[] activeUserRows = activeUserDF.collect();

        System.out.println("find most common device");
        for (Row row : activeUserRows) {
            System.out.println(row);
        }
    }

    /*
    private static void findActiveWindow(SQLContext sqlContext) {
        DataFrame activeWindowDF =
            sqlContext
                .sql("select actor.twtterTimeZone, substr(postedTime, 0, 9), acount(*) as total_count from tweetTable"
                    + "where actor.twitterTimeZone IS NOT NULL"
                    + "Group by actor.twitterTimeZone, substr(postedTime, 0, 9)"
                    + "order by total_count desc" + "limit 15");

        Row[] activeWindowRows = activeWindowDF.collect();
        for (Row row : activeWindowRows) {
            System.out.println(row);
        }
    }

    private static void findStartEndTime(SQLContext sqlContext) {
        // More active User
        Row startTime =
                sqlContext.sql(
                        "select timestampMs as ts as cnt from tweetTable"
                                + "where timestampsMs<> '' order by ts DESC limit 1").collect()[0];

        Row endTime =
                sqlContext.sql(
                        "select timestampMs as ts as cnt from tweetTable"
                                + "where timestampsMs<> '' order by ts ASC limit 1").collect()[0];
    }
    */

}
