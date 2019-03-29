package recommenderSystem

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame


object ItemBasedCFRecommender {
  // Given movieId and the iterable of ratings (user, rating) for all the movies, return the ratings for the specified movieId
  def getMovieRatings(movieId: Int, movieUsersRatings: Iterable[(Int, Iterable[(Int, Float)])]): Iterable[(Int, Float)] = {
    movieUsersRatings.filter(_._1 == movieId).head._2
  }

  // Return the top k similar movies (and their similarity score) to the specified movieId
  def topKSimilarItems(movieId: Int, movieUsersRatings: Seq[(Int, Iterable[(Int, Float)])], k: Int): Iterable[(Int, Float)] = {
    val specifiedMovieRatings = getMovieRatings(movieId, movieUsersRatings)
    // The similarity is computed through a "sparse" Cosine Similarity applied on the rating iterables of
    // the specified movieId and its counterpart
    val res = movieUsersRatings.map(
      m => (
        m._1,
        Utils.sparseCosineSimilarity(
          specifiedMovieRatings,
          m._2
        )
      )
    ).sortWith(_._2 > _._2).take(k)
    res
  }

  // Compute the predicted rating given the iterables of similarities and ratings
  def computePredictedRating(similSeq: Iterable[(Int, Float)], ratSeq: Iterable[(Int, Float)]): Float =  {
    val numRes = Utils.sparseDotProduct(similSeq, ratSeq)
    // The denominator is equal to the sum of the "used" similarities
    val denRes = similSeq.filter(m1 => ratSeq.exists(m2 => m1._1 == m2._1)).map(_._2).sum
    if (denRes == 0)
      return numRes
    numRes/denRes
  }

  // First implementation of Item-based Collaborative Filtering Method
  // Return for each user the (sorted) sequence of movie recommendations and their predicted rating
  // The recommendations are computed using the Item-based Collaborative Filtering method
  // and only not yet seen movies are going to be suggested
  def itemBasedCollaborativeFilteringRecommendations(rdf: DataFrame, mdf: DataFrame, sparkContext: SparkContext, numberOfSimilarItemsToKeepInMatrix: Int = 100): RDD[(Int, Seq[(String, Float)])] = {
    // Get from the ratings DataFrame the "userId", "movieId", "rating" columns in order to compute
    // the RDD with the different ratings (userId, movieId, rating)
    var columnsToSelect = Seq("userId", "movieId", "rating")
    val usersMoviesRatings = rdf.select(columnsToSelect.head, columnsToSelect.tail: _*)
      .rdd.map(r => (r(0).toString.toInt, r(1).toString.toInt, r(2).toString.toFloat))
    // Compute the RDD with the sequence of already seen movies for each user
    val usersAlreadySeenMovies = usersMoviesRatings.map(t => (t._1, t._2)).groupByKey()

    // Compute the distinct movies with at least one rating
    val moviesWithRatings = usersMoviesRatings.map(_._2).distinct().collect()
    // Get from the movies DataFrame the "movieId", "title" columns in order to compute
    // the RDD with each movieId and the corresponding title (with at least one rating)
    columnsToSelect = Seq("movieId", "title")
    val moviesAndTitles = mdf.select(columnsToSelect.head, columnsToSelect.tail: _*)
      .rdd.map(r => (r(0).toString.toInt, r(1).toString))
      .filter(movie => moviesWithRatings.contains(movie._1)).persist()
    // Compute the RDD with the sequence of yet seen movies for each user
    val movies = moviesAndTitles.map(_._1).collect()
    val usersNotYetSeenMovies = usersAlreadySeenMovies.map(
      t => (t._1, movies.filter(movie => !t._2.exists(alreadySeenMovie => alreadySeenMovie == movie)))
    )

    // Compute the ratings (and the reviewer) for each movie
    val movieUsersRatings = usersMoviesRatings.groupBy(_._2).map(t1 => (t1._1, t1._2.map(t2 => (t2._1, t2._3)))).collect()
    // Compute sparse similarity matrix:
    // The sequence of k most similar movies (and their similarity score) has returned for each movie
    // then a Map is computed from this sequence
    val sparseSimilarityMatrix = sparkContext.parallelize(movies).map(m => (m, topKSimilarItems(m, movieUsersRatings, numberOfSimilarItemsToKeepInMatrix))).collect().toMap

    // Create map that given a movieId returns the corresponding title
    val moviesMap = moviesAndTitles.collect().toMap
    // Create map that given a tuple (userId, movieId) returns the corresponding rating, if exists,
    // or 0 otherwise
    val ratingsMap = usersMoviesRatings.map(t => ((t._1, t._2), t._3)).collect()
      .toMap.withDefaultValue(0.toFloat)
    // Create map that given a userId returns the sequence of already seen movies for it
    val usersAlreadySeenMoviesMap = usersAlreadySeenMovies.collect().toMap

    // Compute the movies recommendations:
    // RDD with tuples in the form of (userId, <sequence of (<movie title>, <predicted rating for the movie>)>
    val moviesRecommendations = usersNotYetSeenMovies.map(
      t => {
        val user = t._1
        var moviePredictedRatings = t._2.map(
          // For each not yet seen movie return the title and the predicted rating
          movie => (
            moviesMap(movie),
            // The predicted rating is computed through the similarity of the movie with the ones (movies) that
            // the user has already seen and their assigned rating from the user
            computePredictedRating(
              // Get the similarities for the movie from sparse similarity matrix
              sparseSimilarityMatrix(movie),
              // Get the sequence of the already seen movies and their rating for the user
              usersAlreadySeenMoviesMap(t._1).map(
                seenMovie => (seenMovie, ratingsMap((t._1, seenMovie))))
            )
          )
        ).sortWith(_._2 > _._2).toSeq
        (user, moviePredictedRatings)
      }
    )
    moviesRecommendations
  }

  // Second implementation of Item-based Collaborative Filtering Method
  // Return for each user the (sorted) sequence of movie recommendations and their predicted rating
  // The recommendations are computed using the Item-based Collaborative Filtering method
  // and only not yet seen movies are going to be suggested
  def newItemBasedCollaborativeFilteringRecommendations(rdf: DataFrame, mdf: DataFrame, sparkContext: SparkContext, numberOfSimilarItemsToKeepInMatrix: Int = 100): RDD[(Int, Seq[(String, Float)])] = {
    // Get from the ratings DataFrame the "userId", "movieId", "rating" columns in order to compute
    // the RDD with the different ratings (userId, movieId, rating)
    var columnsToSelect = Seq("userId", "movieId", "rating")
    val usersMoviesRatings = rdf.select(columnsToSelect.head, columnsToSelect.tail: _*)
      .rdd.map(r => (r(0).toString.toInt, r(1).toString.toInt, r(2).toString.toFloat))
    // Compute the RDD with the sequence of already seen movies for each user
    val usersAlreadySeenMovies = usersMoviesRatings.map(t => (t._1, t._2)).groupByKey()

    // Compute the distinct movies with at least one rating
    val moviesWithRatings = usersMoviesRatings.map(_._2).distinct().collect()
    // Get from the movies DataFrame the "movieId", "title" columns in order to compute
    // the RDD with each movieId and the corresponding title (with at least one rating)
    columnsToSelect = Seq("movieId", "title")
    val moviesAndTitles = mdf.select(columnsToSelect.head, columnsToSelect.tail: _*)
      .rdd.map(r => (r(0).toString.toInt, r(1).toString))
      .filter(movie => moviesWithRatings.contains(movie._1)).persist()
    // Compute the RDD with the sequence of yet seen movies for each user
    val movies = moviesAndTitles.map(_._1).collect()
    val usersNotYetSeenMovies = usersAlreadySeenMovies.map(
      t => (t._1, movies.filter(movie => !t._2.exists(alreadySeenMovie => alreadySeenMovie == movie)))
    )

    // Compute the ratings (and the reviewer) for each movie
    val movieUsersRatings = usersMoviesRatings.groupBy(_._2).map(t1 => (t1._1, t1._2.map(t2 => (t2._1, t2._3)))).collect()
    // Compute sparse similarity matrix:
    // The sequence of k most similar movies (and their similarity score) has returned for each movie
    val sparseSimilarityMatrix = sparkContext.parallelize(movies).map(m => (m, topKSimilarItems(m, movieUsersRatings, numberOfSimilarItemsToKeepInMatrix))).collect()

    // Create map that given a movieId returns the corresponding title
    val moviesMap = moviesAndTitles.collect().toMap
    // Create map that given a tuple (userId, movieId) returns the corresponding rating, if exists,
    // or 0 otherwise
    val ratingsMap = usersMoviesRatings.map(t => ((t._1, t._2), t._3)).collect()
      .toMap.withDefaultValue(0.toFloat)
    // Create map that given a userId returns the sequence of already seen movies for it
    val usersAlreadySeenMoviesMap = usersAlreadySeenMovies.collect().toMap
    // Create an RDD for each user with tuples in the form of:
    // (<userId>, <sequence of not yet seen movies>, <sparse similarity matrix with only rows about the not yet seen movies>)
    val usersNotYetSeenMoviesAndSimilarity = usersNotYetSeenMovies.map(
      t => (t._1, t._2, sparseSimilarityMatrix.filter(m => t._2.contains(m._1)))
    )
    // Compute the movies recommendations:
    // RDD with tuples in the form of (userId, <sequence of (<movie title>, <predicted rating for the movie>)>
    val moviesRecommendations = usersNotYetSeenMoviesAndSimilarity.map(
      t => {
        val user = t._1
        var moviePredictedRatings = t._2.map(
          // For each not yet seen movie return the title and the predicted rating
          movie => (
            moviesMap(movie),
            // The predicted rating is computed through the similarity of the movie with the ones (movies) that
            // the user has already seen and their assigned rating from the user
            computePredictedRating(
              // Get the similarities for the movie from sparse similarity matrix
              t._3.filter(mat => mat._1 == movie).head._2,
              // Get the sequence of the already seen movies and their rating for the user
              usersAlreadySeenMoviesMap(t._1).map(
                seenMovie => (seenMovie, ratingsMap((t._1, seenMovie)))
              )
            )
          )
        ).sortWith(_._2 > _._2).toSeq
        (user, moviePredictedRatings)
      }
    )
    moviesRecommendations
  }
}
