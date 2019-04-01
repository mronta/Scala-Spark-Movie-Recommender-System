package recommenderSystem


object MovieRecommenderSystem {
  def main(args:Array[String]): Unit = {
    // Initialize system and required paths
    val (sparkSess, sparkCon, movieDataPath, ratingDataPath) = Utils.initializeSystem()
    // Get the required DataFrames from the corresponding csv files
    val moviesDf = Utils.returnDataFrameFromCSV(sparkSess, movieDataPath)
    val ratingsDf = Utils.returnDataFrameFromCSV(sparkSess, ratingDataPath)
    // Prepare the rating DataFrame in order to compute the movie recommendations
    val ratingsDfPrepared = Utils.ratingsDataFramePreparation(ratingsDf, sparkCon, false)
    // Number of movies recommendations for each user
    val recommendationsNumber = 5
    // Number of users for which return the movies recommendations
    val userNumber = 5
    // Number of similar items to keep in the similarity matrix
    // (parameter for the Item-based method and Hybrid method that use the Item-based)
    val numberOfSimilarItemsToKeepInMatrix = 100

    // Displaying movies recommendations for each implemented method and their required time
    var results = "DIFFERENT RECOMMENDER SYSTEMS RESULTS\n" + "---\n\n"
    var t0: Long = 0
    var t1: Long = 0

    t0 = System.nanoTime()
    results += Utils.displayRecommendations(
      "Random Recommender",
      RandomRecommender.randomRecommendations(recommendationsNumber, ratingsDfPrepared, moviesDf),
      userNumber,
      recommendationsNumber
    )
    t1 = System.nanoTime()
    results += "time elapsed: " + ((t1-t0)/Math.pow(10,9)) + "s\n\n"
    
    t0 = System.nanoTime()
    results +=Utils.displayRecommendations(
      "New Item-based Collaborative Filtering Recommender",
      ItemBasedCFRecommender.newItemBasedCollaborativeFilteringRecommendations(ratingsDfPrepared, moviesDf, sparkCon, numberOfSimilarItemsToKeepInMatrix),
      userNumber,
      recommendationsNumber
    )
    t1 = System.nanoTime()

    results += "time elapsed: " + ((t1-t0)/Math.pow(10,9)) + "s\n\n"
    t0 = System.nanoTime()
    results +=Utils.displayRecommendations(
      "Item-based Collaborative Filtering Recommender",
      ItemBasedCFRecommender.itemBasedCollaborativeFilteringRecommendations(ratingsDfPrepared, moviesDf, sparkCon, numberOfSimilarItemsToKeepInMatrix),
      userNumber,
      recommendationsNumber
    )
    t1 = System.nanoTime()
    results += "time elapsed: " + ((t1-t0)/Math.pow(10,9)) + "s\n\n"

    t0 = System.nanoTime()
    results +=Utils.displayRecommendations(
      "Content Based Recommender",
      ContentBasedRecommender.contentBasedRecommendations(ratingsDfPrepared, moviesDf),
      userNumber,
      recommendationsNumber
    )
    t1 = System.nanoTime()
    results += "time elapsed: " + ((t1-t0)/Math.pow(10,9)) + "s\n\n"

    t0 = System.nanoTime()
    results +=Utils.displayRecommendations(
      "Hybrid Filtering Recommender",
      // weights taken from paper "Recommender system techniques applied to Netflix movie data"
      HybridFilteringRecommender.hybridFilteringRecommendations(ratingsDfPrepared, moviesDf, sparkCon, 0.6.toFloat, 0.4.toFloat, numberOfSimilarItemsToKeepInMatrix),
      userNumber,
      recommendationsNumber
    )
    t1 = System.nanoTime()
    results += "time elapsed: " + ((t1-t0)/Math.pow(10,9)) + "s\n\n"

    println(results)

    val resultsRdd = sparkCon.parallelize(Seq(results)).coalesce(1)
    resultsRdd.saveAsTextFile(Utils.bucketPath + "results")
  }
}
