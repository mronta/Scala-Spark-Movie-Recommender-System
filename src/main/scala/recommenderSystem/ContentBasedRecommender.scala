package recommenderSystem

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType


object ContentBasedRecommender {

  def contentBasedRecommendations(rdf: DataFrame, mdf: DataFrame): RDD[(Int, Seq[(String, Float)])] = {

    //For content-based filtering, only the 5 most recent movie ratings rated with a 3 or higher are taken into consideration.
    val ratingsConsidered = selectMostRecentMovieRatingsRatedWith3orHigher(mdf, rdf)

    //genre information matrix G is a m × k matrix with m movies and k genres. Matrix G is filled with 1 if movie m belongs to genre k, 0 otherwise
    val movieGenresMatrix = buildMoviesGenresMatrix(mdf)

    //The outcome of the dot product between the rating matrix and genre matrix is a n × k matrix P that contains the predisposition of each user towards each genre
    val predispositionOfEachUserTowardsEachGenre = calculatePredispositionOfEachUserTowardsEachGenre(ratingsConsidered, movieGenresMatrix)

    //Compute already seen movies for each user
    val usersAlreadySeenMovies = calculateUsersAlreadySeenMovies(rdf)

    val recommendationWithMovieId = calculateContendBasedRecommendation(predispositionOfEachUserTowardsEachGenre, movieGenresMatrix, usersAlreadySeenMovies)

    val recommendationWithMovieTitle = findMovieTitleForRecommendation(mdf, recommendationWithMovieId)

    recommendationWithMovieTitle

  }

  //For content-based filtering, only the 5 most recent movie ratings rated with a 3 or higher are taken into consideration.
  def selectMostRecentMovieRatingsRatedWith3orHigher(moviesDf: DataFrame, ratingsDf: DataFrame, numberOfMostRecentRatingsConsidered:Int = 5) : RDD[(Int, Seq[(Int, Float)])] = {

    val idOfMoviesWithoutGenres =
      moviesDf
        .select("movieId", "genres")
        .rdd
        // 0: movieId - 1: genres
        .filter(row => row.getString(1) == "(no genres listed)")
        //maintain only movieId and convert it in Int
        .map(_.getString(0).toInt)
        .collect()

    ratingsDf
      //select only ratings rated with a 3 or higher on movies for which we know the genres
      .filter(ratingsDf("rating").cast(IntegerType) >= 3 && !ratingsDf("movieId").cast(IntegerType).isin(idOfMoviesWithoutGenres: _*))
      .rdd
      .map(row => {
        val userId = row.getString(0).toInt
        val movieId = row.getString(1).toInt
        val rating = row.getString(2).toFloat
        val timestamp = row.getString(3).toInt

        (userId, (movieId, rating, timestamp))
      })
      //group data foreach user
      .groupByKey()
      .map(element => (
        //maintain the userId as key in the output rdd
        element._1,
        element._2.toSeq
          //order ratings using timestamp
          .sortBy(_._3)(Ordering[Int].reverse)
          //maintain only the most recent ratings
          .take(numberOfMostRecentRatingsConsidered)
          //remove the timestamp that is unnecessary
          .map(triple => (triple._1, triple._2))))

  }

  //genre information matrix G is a m × k matrix with m movies and k genres. Matrix G is filled with 1 if movie m belongs to genre k, 0 otherwise
  def buildMoviesGenresMatrix(moviesDf: DataFrame, arrayOfGenres: Array[String] = Array("Action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "IMAX", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western")) : Map[Int, Array[Float]] = {

    moviesDf
      .select("movieId", "genres")
      .rdd
      //remove movies without genres information (0: movieId - 1: genres)
      .filter(row => row.getString(1) != "(no genres listed)")
      .map(row => {
        val filmId = row.getString(0).toInt
        val currentMovieGenresArray = row.getString(1).split('|')
        //initialize a matrix row foreach movie
        val currentMovieGenresRow = Array.fill(arrayOfGenres.length)(0.toFloat)
        //update the matrix row for current movie genres
        currentMovieGenresArray.foreach(genre => {
          val i = arrayOfGenres.indexOf(genre)
          currentMovieGenresRow(i) = 1.toFloat
        })
        //return a rdd that represent the genres matrix required
        (filmId, currentMovieGenresRow)
      })
      .collect()
      .toMap.withDefaultValue(Array.fill(arrayOfGenres.length)(0.toFloat))

  }

  //The outcome of the dot product between the rating matrix and genre matrix is a n × k matrix P that contains the predisposition of each user towards each genre
  def calculatePredispositionOfEachUserTowardsEachGenre(ratingsConsidered: RDD[(Int, Seq[(Int, Float)])], movieGenresMatrix: Map[Int, Array[Float]]): RDD[(Int, Array[Float])] = {

    ratingsConsidered
      //row: userId, List((movieId, rating), ....)
      .map(row => (
                    row._1,
                    //select the movieGenres row corresponding to the movie rated and multiply each value with the corresponding rating
                    row._2.map(singleRating => movieGenresMatrix(singleRating._1).map(_ * singleRating._2))
                  )
      )
      //sum for each user all the single movie genre predisposition array obtained
      .map(userInformation => ( userInformation._1,   userInformation._2.reduce((array1,array2) => array1.zip(array2).map { case (x, y) => x + y })   ))

  }

  //Compute already seen movies for each user
  def calculateUsersAlreadySeenMovies(ratingsDf: DataFrame) : Map[Int, Set[Int]] = {

    ratingsDf
      .select("userId", "movieId")
      .rdd
      .map(row => {
        val userId = row.getString(0).toInt
        val movieId = row.getString(1).toInt

        (userId, movieId)
      })
      //group data foreach user
      .groupByKey()
      .map(element => (element._1, element._2.toSet))
      .collect()
      .toMap.withDefaultValue(Set())

  }

  //Next, based on this predisposition matrix P, a recommendation for a user can be made by calculating the cosine similarity between the user profile vector
  //(the u-th row of matrix P for user u) and the genre information matrix G.
  def calculateContendBasedRecommendation(predispositionOfEachUserTowardsEachGenre: RDD[(Int, Array[Float])], movieGenresMatrix: Map[Int, Array[Float]], usersAlreadySeenMovies: Map[Int, Set[Int]]) : RDD[(Int, Seq[(Int, Float)])] = {

    (
      for{
        userPredispositionArray <- predispositionOfEachUserTowardsEachGenre
        //it does not consider already seen movies
        //obtain the set of already seen movies for the current user
        setAlreadySeenMoviesCurrentUser = usersAlreadySeenMovies(userPredispositionArray._1)
        //in the genre information matrix consider only the not seen movies
        movieGenresArray <- movieGenresMatrix.filter(movieInformation => !setAlreadySeenMoviesCurrentUser.contains(movieInformation._1)).toSeq
      }
      //return userID, (movieId, recommendationValue)
      yield (userPredispositionArray._1, (movieGenresArray._1, Utils.cosineSimilarity(userPredispositionArray._2, movieGenresArray._2)))
    )
    //group data foreach user
    .groupByKey()
    //foreach user order the recommendation list
    .map(userRecommendation => {
      val user = userRecommendation._1
      val movieScores = userRecommendation._2.toSeq.map(t => (t._1, t._2*5)).sortBy(_._2)(Ordering[Float].reverse)
      (user, movieScores)
    })

  }

  def findMovieTitleForRecommendation(moviesDf: DataFrame, recommendationWithMovieId: RDD[(Int, Seq[(Int, Float)])]) : RDD[(Int, Seq[(String, Float)])] = {
    // Compute movies and titles RDD
    val moviesAndTitles = moviesDf.select("movieId", "title").rdd.map(r => (r(0).toString.toInt, r(1).toString))
    // Create map that given a movieId return the related title
    val moviesMap = moviesAndTitles.collect().toMap

    recommendationWithMovieId.map(row => (row._1, row._2.map(movieRecommendation => (moviesMap(movieRecommendation._1), movieRecommendation._2))))
  }

}
