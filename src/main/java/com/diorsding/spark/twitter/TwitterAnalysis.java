package com.diorsding.spark.twitter;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;

/**
 * Read Spark Cassandra Connector Doc Carefully. All are needed.
 * https://github.com/datastax/spark-cassandra-connector/tree/master/doc
 *
 * @author jiashan
 *
 */
public class TwitterAnalysis {

    public static void main(String[] args) {
        SparkConf sparkConf =
                new SparkConf(true).set("spark.cassandra.connect.host", "localhost")
                        .set("spark.cassandra.auth.username", "cassandra")
                        .set("spark.cassandra.auth.username", "cassandra");

        SparkContext sc = new SparkContext(" ", " ", sparkConf);

        // Read Cassandra Data.
        JavaRDD<String> cassandraRowsRdd =
                CassandraJavaUtil.javaFunctions(sc).cassandraTable("ks", "table")
                        .map(new Function<CassandraRow, String>() {
                            public String call(CassandraRow row) throws Exception {
                                return row.getString("name"); // row -> any column or tuple.
                            }
                        });


        // Register Temp Table


        // Spark SQL - do analysis
        // List any insights here.


        // How to handle results? store them in mySQL
    }

}
