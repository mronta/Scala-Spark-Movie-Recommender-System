package recommenderSystem

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object HybridFilteringRecommender {
  // Given Item-based and Content-based movies recommendations and the weights to apply for them
  // return the movies recommendations for the Hybrid Filtering method
  def computeHybridScore(itemBasedRDD: RDD[(Int, Seq[(String, Float)])], contentBasedRDD: RDD[(Int, Seq[(String, Float)])], itemBasedWeight: Float, contentBasedWeight: Float): RDD[(Int, Seq[(String, Float)])] = {

    itemBasedRDD.join(contentBasedRDD).map{
      //join(contentBasedRDD).map{
      case (key, (itemSeq, contentSeq)) => {
        // For each movie the score is computed. The formula is:
        // Item-based score * 'itemBasedWeight' + Content-based score * 'contentBasedWeight'
        val contentSeqMap = contentSeq.toMap.withDefaultValue(0.toFloat)
        val results = itemSeq.map(t =>
          (t._1, t._2*itemBasedWeight + contentSeqMap(t._1)*contentBasedWeight)
        )
        (key, results)
      }
    }
  }

  // Return for each user the (sorted) sequence of movie recommendations and their predicted rating
  // The recommendations are computed using the Hybrid Filtering method that exploit the results of
  // Item-based Collaborative Filtering and Content Based Filtering
  // Only not yet seen movies are going to be suggested
  def hybridFilteringRecommendations(rdf: DataFrame, mdf: DataFrame, sparkCon: SparkContext, itemBasedWeight: Float = 0.6.toFloat, contentBasedWeight: Float = 0.4.toFloat,
                                     numberOfSimilarItemsToKeepInMatrix: Int = 100): RDD[(Int, Seq[(String, Float)])] = {
    // Compute movies recommendations with Item-based Collaborative Filtering method
    val itemBasedCFResults = ItemBasedCFRecommender.itemBasedCollaborativeFilteringRecommendations(rdf, mdf, sparkCon, numberOfSimilarItemsToKeepInMatrix)
    // Compute movies recommendations with Content-based Filtering method
    val contentBasedResults = ContentBasedRecommender.contentBasedRecommendations(rdf, mdf)
    // Compute for each user the movies recommendations weighting scores from the previous methods with the values of
    // "itemBasedWeight" and "contentBasedWeight" parameters (weight values have to be between 0 and 1)
    val moviesRecommendations = computeHybridScore(itemBasedCFResults, contentBasedResults, itemBasedWeight, contentBasedWeight)
        .map(t => {
          val user = t._1
          val moviePredictedRatings = t._2.sortWith(_._2 > _._2)
          (user, moviePredictedRatings)
        })
    moviesRecommendations
  }
}
