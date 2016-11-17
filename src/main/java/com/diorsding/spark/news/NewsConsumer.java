package com.diorsding.spark.news;

import org.apache.spark.SparkContext;

/**
 * @author jiashan
 *
 * Use Spark to read mongo data
 *
 */
public class NewsConsumer {

    public static void main(String[] args) {
        SparkContext sc = new SparkContext("local", "Scala word Count");

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
