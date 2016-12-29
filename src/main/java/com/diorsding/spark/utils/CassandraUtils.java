package com.diorsding.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import com.diorsding.spark.twitter.Constants;
import com.diorsding.spark.twitter.Tweet;

import java.util.Date;

public class CassandraUtils {

    public static void setupCanssadraTables() {
        SparkConf sc = new SparkConf();
        sc.set(Constants.CASSANDRA_CONNECTION_HOST_KEY, Constants.CASSANDRA_CONNECTION_HOST_VALUE);

        CassandraConnector connector = CassandraConnector.apply(sc);
        Session session = connector.openSession();

        // 1. Create Keyspace
        String createKeyspaceSQL =
            String
                .format("create keyspace if not exists %s with replication = {'class':'SimpleStrategy', 'replication_factor':1};",
                    Constants.CASSANDRA_TWITTER_KEYSPACE);
        session.execute(createKeyspaceSQL);

        // 2. Create Table
        // Cassandra doesn't assume default ordering for the other clustering keys so we have to specify it.
        String createTableSQL =
            String.format(
                "create table if not exists %s.%s (id bigint, user text, screen_name text, profile_image_url text, text text, " +
                    "latitude double, longitude double, language text, device text, score int, date timestamp, PRIMARY KEY((date), user)"
                    + ") WITH CLUSTERING ORDER BY (user ASC);", Constants.CASSANDRA_TWITTER_KEYSPACE,
                Constants.CASSANDRA_TWITTER_TABLE);

        session.execute(createTableSQL);

        // Seems mvn exec:java -Dexec.mainClass="com.diorsding.spark.utils.CassandraUtils"
        // won't exit immediately after running sql.
        session.close();
    }

    public static void dumpTweetsToCassandra(JavaDStream<Tweet> tweets) {
        /*
         * Write data to Cassandra. Since what we get is JavaDStream from Streaming, we use CassandraStreamingJavaUtil
         * instead. Do not use RDD here. It make problem complex because we need to convert DStream to RDD again.
         *
         */

        CassandraStreamingJavaUtil.javaFunctions(tweets)
            .writerBuilder(Constants.CASSANDRA_TWITTER_KEYSPACE, Constants.CASSANDRA_TWITTER_TABLE, CassandraJavaUtil.mapToRow(Tweet.class))
            .saveToCassandra();
    }
}
