package com.diorsding.spark.ml;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;

/**
 * Step by step generated DataFrames are useless. This is just helpful for observe data. 
 * We have pipeline to help us chain all the stages together.
 * 
 * @author jiashan
 *
 */
public class WikiPageClustering {

    public static void main(String[] args) {
        SparkConf sparkConf =
                new SparkConf().setMaster("local[2]").setAppName(WikiPageClustering.class.getSimpleName());
        SparkContext sc = new SparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame wikiDF = sqlContext.read().parquet("location").cache();
        // How to select all columns (String * and Column type)
        DataFrame wikiLoweredDF = wikiDF.select(functions.lower(wikiDF.col("text")).alias("lowerText"));

        // Step 1: Tokenizer
        RegexTokenizer tokenizer =
                new RegexTokenizer().setInputCol("lowerText").setOutputCol("words").setPattern("\\W+");
        // DataFrame wikiWordsDF = tokenizer.transform(wikiLoweredDF);

        // Step 2: Remove Stop Words
        StopWordsRemover remover = new StopWordsRemover().setInputCol("words").setOutputCol("noStopWords");
        // DataFrame noStopWordsListDf = remover.transform(wikiWordsDF);

        // Step 3: HashingTF
        int numFeatures = 20000;
        HashingTF hashingTF =
                new HashingTF().setInputCol("noStopWords").setOutputCol("hashingTF").setNumFeatures(numFeatures);
        // DataFrame featurizedDF = hashingTF.transform(noStopWordsListDf);

        // Step 4: IDF
        IDF idf = new IDF().setInputCol("hashingTF").setOutputCol("idf");
        // IDFModel idfModel = idf.fit(featurizedDF);

        // Step 5: Normalizer
        Normalizer normalizer = new Normalizer().setInputCol("idf").setOutputCol("features");

        // Step 6: KMeans
        int numCluster = 100;
        KMeans kmeans =
                new KMeans().setFeaturesCol("features").setPredictionCol("prediction").setK(numCluster).setSeed(0);

        // Step 7: ML Pipeline Training model.
        List<PipelineStage> pipelineStages = Arrays.asList(tokenizer, remover, hashingTF, idf, normalizer, kmeans);
        Pipeline pipeline = new Pipeline().setStages(pipelineStages.toArray(new Pipeline[] {}));
        PipelineModel model = pipeline.fit(wikiLoweredDF);

        // TODO: store trained model and then we can reuse next time.

        // Step 8: Use trained model to predict new data frames
        DataFrame predictionDF = model.transform(wikiLoweredDF);

        predictionDF.printSchema();
    }

}
