package recommenderSystem

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object RandomRecommender {
  // Get k distinct random elements from a sequence
  def getKRandomElementsFromSeq(seq: Seq[Int], k: Int): Seq[Int] = {
    val rand = scala.util.Random
    val seqLen = seq.size
    if (seqLen < k)
      return seq
    var elementsSet: Set[Int] = Set()
    while (elementsSet.size < k) {
      elementsSet = elementsSet + seq(rand.nextInt(seqLen))
    }
    elementsSet.toSeq
  }

  // Return the seq movie titles with related recommendation score (predicted rating)
  // Already seen movies are not returned and the recommendation score is random
  def randomRecommendations(recommendationsNumber: Int, rdf: DataFrame, mdf: DataFrame): RDD[(Int, Seq[(String, Float)])] = {
    val rand = scala.util.Random
    // Compute movies and titles and already seen movies for each user RDDs
    var columnsToSelect = Seq("movieId", "title")
    val moviesAndTitles = mdf.select(columnsToSelect.head, columnsToSelect.tail: _*)
      .rdd.map(r => (r(0).toString.toInt, r(1).toString))
    columnsToSelect = Seq("userId", "movieId")
    val usersAlreadySeenMovies = rdf.select(columnsToSelect.head, columnsToSelect.tail: _*)
      .rdd.map(r => (r(0).toString.toInt, r(1).toString.toInt)).groupByKey()
    // Compute not yet seen movies RDD
    val movies = moviesAndTitles.map(_._1).collect()
    val usersNotYetSeenMovies = usersAlreadySeenMovies.map(
      t => (t._1, movies.filter(movie => !t._2.exists(alreadySeenMovie => alreadySeenMovie == movie)))
    )
    // Create map that given a movieId return the related title
    val moviesMap = moviesAndTitles.collect().toMap
    // Return k random movies (and score for them) for each user
    val moviesRecommendations = usersNotYetSeenMovies.map(
      t => (t._1, getKRandomElementsFromSeq(t._2.toSeq, recommendationsNumber))
    ).map(t => (t._1, t._2.map(e => (moviesMap(e), rand.nextFloat + rand.nextInt(4) + 1)).toArray.toSeq))
    moviesRecommendations
  }
}
