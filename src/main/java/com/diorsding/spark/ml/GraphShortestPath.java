package com.diorsding.spark.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * No Java Version API available now. Maven lib is used to serve scala.
 * I will have some scala sample codes here.

Vertices
+--------+--------------------+
|      id|            id_title|
+--------+--------------------+
|10569372|      1969_Davis_Cup|
| 6791390|2005_FIBA_America...|
|45445712|2014-15_CR_Beloui...|
|31609470|               A1one|
|41523582|   Abbeyfield_School|
+--------+--------------------+

Edges
+--------+--------+-----+---+--------------------+--------------------+
|     src|     dst| type|  n|           src_title|           dst_title|
+--------+--------+-----+---+--------------------+--------------------+
|  600744| 2556962| link|297|                 !!!|         !!!_(album)|
|11327948| 1261557|other|482|              Heroes|              Heroes|
|26182691|28612415|other|294|  Watkin_Tudor_Jones|$O$_(Die_Antwoord...|
|       1|45238665|other|199|     other-wikipedia|                 &TV|
|  933444|24903940| link| 33|Operation_Ivy_(band)|         '69_Newport|
+--------+--------+-----+---+--------------------+--------------------+
 * 
 * @author jiashan
 *
 */
public class GraphShortestPath {

    public static void main(String[] args) {
        SparkConf sparkConf =
                new SparkConf().setMaster("local[2]").setAppName(WikiPageClustering.class.getSimpleName());
        SparkContext sc = new SparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);


        DataFrame verticesDF = sqlContext.read().parquet("edgesDFPath").cache();

        DataFrame edgesDf = sqlContext.read().parquet("edgesDFPath").cache();


        /**
         *
         * import org.graphframes._
         * val clickstreamGRAPH = GraphFrame(verticesDF, edgesDF)
         * 
         * Remove useless vertice
         * val verticesSubgraphDF = clickstreamGRAPH.vertices.filter($"id_title" !== "Main_Page")
         * 
         * Remove Main_Page and non
         * edgesDF.filter("n > '300'").filter($"src" !== 15580374).filter($"dst" !== 15580374).count
         *
         *
         * Clean Data
         * val edgesSubgraphDF2 = edgesDF
            .filter($"src" !== 15580374) //remove main page
            .filter($"dst" !== 15580374)
            .filter($"src_title" !== "other-google")
            .filter($"src_title" !== "other-wikipedia")
            .filter($"src_title" !== "other-empty")
            .filter($"src_title" !== "other-bing")
            .filter($"src_title" !== "other-twitter")
            .filter($"src_title" !== "other-facebook")
            .filter($"src_title" !== "other-other")
            .filter($"src_title" !== "other-yahoo")
            .filter($"src_title" !== "other-internal")
         *
         * val clickstreamSubGRAPH = GraphFrame(verticesSubgraphDF, edgesSubgraphDF2)
         * 
         * Find Id
         * clickstreamGRAPH.vertices.filter($"id_title" === "Apache_Spark").show
         * clickstreamGRAPH.vertices.filter($"id_title" === "Kevin_Bacon").show
         * 
         * Result will have multiple paths
         * val shortestPathDF =  clickstreamSubGRAPH.bfs.fromExpr("id = 42164234").toExpr("id = 16827").run() 
         *
         */



    }
}
