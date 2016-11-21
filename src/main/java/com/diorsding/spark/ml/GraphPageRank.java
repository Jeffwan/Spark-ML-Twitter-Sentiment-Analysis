package com.diorsding.spark.ml;

public class GraphPageRank {

    
    /**
     * The way to construct clickstreamGraph is same as the steps in shortestPath class. 
     * 
     * val prGRAPH = clickstreamGRAPH.pageRank.maxIter(3).run()  //621.45s to run 
     * prGRAPH.vertices.show(10)
     * display(prGRAPH.vertices.orderBy($"pagerank".desc).limit(30))
     */
}
