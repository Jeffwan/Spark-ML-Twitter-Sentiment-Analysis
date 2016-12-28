package com.diorsding.spark.utils;

import org.apache.spark.SparkConf;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.diorsding.spark.twitter.Constants;

import java.util.Date;

public class CassandraSetupUtils {

    public static void main(String[] args) {
        SparkConf sc = new SparkConf();
        sc.set("spark.cassandra.connection.host", "10.148.254.9");

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
                    "latitude double, longitude double, score int, date timestamp, PRIMARY KEY((date), user)"
                        + ") WITH CLUSTERING ORDER BY (user ASC);", Constants.CASSANDRA_TWITTER_KEYSPACE,
            Constants.CASSANDRA_TWITTER_TABLE);

        session.execute(createTableSQL);

        // Seems mvn exec:java -Dexec.mainClass="com.diorsding.spark.utils.CassandraSetupUtils"
        // won't exit immediately after running sql.
        session.close();
    }
}
