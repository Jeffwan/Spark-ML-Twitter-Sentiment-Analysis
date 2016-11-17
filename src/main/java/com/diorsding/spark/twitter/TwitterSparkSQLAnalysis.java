package com.diorsding.spark.twitter;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
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
                new SparkConf(true).set("spark.cassandra.connect.host", "localhost")
                        .set("spark.cassandra.auth.username", "cassandra")
                        .set("spark.cassandra.auth.username", "cassandra");
        SparkContext sc = new SparkContext("spark://127.0.0.1:7077", "test", sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // Step 1. Convert Table to RDD and cache.
        readTweetTable(sc, sqlContext);

        // Step 2. SparkSQL Analysis

        getTweetsSample(sqlContext);

        findActiveUsers(sqlContext);

        findActiveWindow(sqlContext);

        // TODO: find influenced user

        // TODO: find most common device
    }

    private static void readTweetTable(SparkContext sc, SQLContext sqlContext) {
        CassandraTableScanJavaRDD<CassandraRow> data =
                CassandraJavaUtil.javaFunctions(sc).cassandraTable("test", "words");
        JavaRDD<CassandraRow> filterEmptyLine = data.filter(new Function<CassandraRow, Boolean>() {
            @Override
            public Boolean call(CassandraRow cassandraRow) throws Exception {
                return cassandraRow.getString("??").trim().isEmpty();
            }
        });

        JavaRDD<String> processedRDD = filterEmptyLine.map(new Function<CassandraRow, String>() {
            @Override
            public String call(CassandraRow row) throws Exception {
                return row.getString("hehe");
            }
        });


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
        Row[] rows = sqlContext.sql("select body from tweetTable Limit 100").collect();
        for (Row row : rows) {
            System.out.println(row);
        }

        DataFrame tweetDF = sqlContext.sql("select body from tweetTable where body <> '' ");
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
