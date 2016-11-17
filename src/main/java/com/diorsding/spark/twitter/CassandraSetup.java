package com.diorsding.spark.twitter;

import org.apache.spark.SparkConf;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

public class CassandraSetup {

    public static void main(String[] args) {

        SparkConf sc = new SparkConf();

        sc.set("cassandra.host", "127.0.0.1");

        CassandraConnector connector = CassandraConnector.apply(sc);

        Session session = connector.openSession();

        session.execute("create keyspace if not exists $keyspace with replication = {'class':'SimpleStrategy', 'replication_factor':1};");

        /*
         * session.execute("create table if not exists $CassandraKeyspace.$CassandraTable ( " + "date timestamp," +
         * "user text," + "text text," + "PRIMARY KEY((data, user));"; + ") WITH CLUSTERING ORDER BY (dimension ASC);";
         */


    }

}
