package com.diorsding.spark.news;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class RTCluster {

    /*
     * println("Initializing Streaming Spark Context...")
     *
     * val conf = new SparkConf().setAppName(this.getClass.getSimpleName)val ssc = new StreamingContext(conf,
     * Seconds(5))
     *
     * val model = new KMeansModel(ssc.sparkContext.objectFile[Vector]( modelFile.toString).collect())
     *
     * val filteredNews = statuses .filter(t => model.predict(Utils.featurize(t)) == clusterNumber) filteredNews.print()
     */

    public static void main(String[] args) {
        System.out.println("Initializing Streaming Spark Context...");
        SparkConf sc = new SparkConf().setAppName(RTCluster.class.getSimpleName()).setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(sc, Seconds.apply(5000));
    }

}
