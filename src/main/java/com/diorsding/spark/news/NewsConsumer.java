package com.diorsding.spark.news;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.bson.BSONObject;

import scala.Tuple2;

import com.mongodb.hadoop.MongoInputFormat;

/**
 * @author jiashan
 *
 * Use Spark to read mongo data
 *
 */
public class NewsConsumer {

    public static <U> void main(String[] args) {
        SparkContext sc = new SparkContext("local", "Scala word Count");

        Configuration config = new Configuration();
        config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/NYtimes.news");


        RDD<Tuple2<Object,BSONObject>> mongoRDD = sc.newAPIHadoopRDD(config, MongoInputFormat.class, Object.class, BSONObject.class);

        // mongoRDD.flatMap(new Function1<Tuple2<Object,BSONObject>, TraversableOnce<>>() {
        //
        //
        // }, evidence$4);

       // Get all information


        // sc.newAPIHadoopRDD(conf, MongoInputFormat.class, BSONObject.class, vClass);
        /**
         * val sc = new SparkContext("local", "Scala Word Count")
         *
         * val config = new Configuration() config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/NYtimes.news")
         *
         * val textRDD = mongoRDD.flatMap(arg => { val docID = arg._2.get("_id") var text = arg._2.get("text").toString
         * text = text.toLowerCase().replaceAll("[.,!?\n]", " ") (docID, text)
         */


    }
}
