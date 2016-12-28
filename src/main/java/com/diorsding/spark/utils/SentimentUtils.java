package com.diorsding.spark.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class SentimentUtils {

    private static StanfordCoreNLP pipeline = null;

    {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment");
        pipeline = new StanfordCoreNLP(props);
    }

    public static String calculateSentimentScore(String text) {
        Annotation annotation = pipeline.process(text);

        List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);

        String sentiment = null;
        for (CoreMap sentence : sentences) {
            sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
        }

        // Calculate total, average and jiaquan

        return sentiment;
    }

    public static int calculateWeightedSentimentScore(String text) {
        Annotation annotation = pipeline.process(text);

        List<Double> sentiments = new ArrayList<Double>();
        List<Integer> sizes = new ArrayList<Integer>();

        List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);

            sentiments.add(sentiment + 0.0);
            sizes.add(String.valueOf(sentiment).length());
        }

        int weightedSentiment = 0; // by default value;

        if (sentiments.isEmpty()) {
            weightedSentiment = -1;
        } else {
            weightedSentiment = 1;
        }

        return normalizeCoreNLPSentiment(weightedSentiment);
    }

    private static int normalizeCoreNLPSentiment(int score) {
        if (score <= 0.0) { //neutral
            return 0;
        }

        if (score < 2.0) { //negative
            return -1;
        }

        if (score < 3.0) { //neutral
            return 0;
        }

        if (score < 5.0) { //positive
            return 1;
        }

        return 0; // can not find the sentiment, give default value
    }
}
