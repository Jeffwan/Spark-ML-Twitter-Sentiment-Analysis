package com.diorsding.spark.twitter;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;

/**
 * Read Spark Cassandra Connector Doc Carefully. All are needed.
 * https://github.com/datastax/spark-cassandra-connector/tree/master/doc
 *
 * @author jiashan
 *
 */

public class TwitterSparkSQLAnalysis {

    public static void main(String[] args) {
        SparkConf sparkConf =
                new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
                        .set("spark.cassandra.connection.host", "10.148.254.9");

        SparkContext sc = new SparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // Step 1. Convert Table to RDD and cache.
        readTweetTable(sc, sqlContext);

        // Step 2. SparkSQL Analysis
        getTweetsSample(sqlContext);

        // - Can not find source data there. Need more complex structure to store tweets.

        // findActiveUsers(sqlContext);

        // findActiveWindow(sqlContext);

        // TODO: find influenced user

        // TODO: find most common device


        sc.stop();
    }

    private static void readTweetTable(SparkContext sc, SQLContext sqlContext) {

        // cassandraTable(Helper.getKeyspace(), Helper.getTable(), CassandraJavaUtil.mapRowTo(Tweet.class)) ->
        // CassandraTableScanJavaRDD<Tweet>
        CassandraTableScanJavaRDD<CassandraRow> data =
                CassandraJavaUtil.javaFunctions(sc).cassandraTable(Helper.getKeyspace(), Helper.getTable());

        JavaRDD<CassandraRow> filterEmptyLine =
                data.filter(cassandraRow -> !cassandraRow.getString("text").trim().isEmpty());

        JavaRDD<String> processedRDD = filterEmptyLine.map(cassandraRow -> cassandraRow.toString());

        DataFrame tweetTable = sqlContext.jsonRDD(processedRDD);

        tweetTable.registerTempTable("tweetTable");

        sqlContext.cacheTable("tweetTable");
    }

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

    private static void getTweetsSample(SQLContext sqlContext) {
        Row[] rows = sqlContext.sql("select * from tweetTable Limit 10").collect();

        for (Row row : rows) {
            System.out.println(row);
        }

        // DataFrame tweetDF = sqlContext.sql("select * from tweetTable limit 5 ");
    }

    private static void findActiveUsers(SQLContext sqlContext) {
        // More active User
        DataFrame activeUserDF =
                sqlContext.sql("select actor.languages,count(*) as cnt from tweetTable"
                        + "group by actor.languages order by cnt des limit 25 ");
        Row[] activeUserRows = activeUserDF.collect();

        for (Row row : activeUserRows) {
            System.out.println(row);
        }

        System.out.print("Q1 completed");
    }


    private static void findStartEndTime(SQLContext sqlContext) {
        // More active User
        Row startTime =
                sqlContext.sql(
                        "select timestampMs as ts as cnt from tweetTable"
                                + "where timestampsMs<> '' order by ts DESC limit 1").collect()[0];

        System.out.print("Q1 completed");

        Row endTime =
                sqlContext.sql(
                        "select timestampMs as ts as cnt from tweetTable"
                                + "where timestampsMs<> '' order by ts ASC limit 1").collect()[0];

        System.out.print("Q1 completed");
    }

}
