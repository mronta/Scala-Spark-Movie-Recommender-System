package recommenderSystem

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {

  //val bucketPath = "s3://movierecommenderbucket/"
  val bucketPath = "../"
  //val datasetFolderPath = "ml-latest-small/"
  val datasetFolderPath = "ml-1m/"

  // Initialize the system
  def initializeSystem() : (SparkSession, SparkContext, String, String) = {
    val sparkSess = SparkSession.builder
      // Comment the line below if the run is on cloud
      .master("local")
      .appName("MovieRecommenderSystem")
      .getOrCreate
    val sparkCon = sparkSess.sparkContext

    val folderDataPath = bucketPath + datasetFolderPath
    val movieDataPath = folderDataPath + "movies.csv"
    val ratingDataPath = folderDataPath + "ratings.csv"

    (sparkSess, sparkCon, movieDataPath, ratingDataPath)
  }

  // Return the Dataframe from the corresponding csv file path
  def returnDataFrameFromCSV(sparkSession: SparkSession, csvPath: String) : DataFrame = {
    val df = sparkSession.read.format("csv").option("header", "true").load(csvPath)
    df
  }

  // Drop the ratings DataFrame rows that have values for the "columnName" column under the quantile in the values distribution
  def removeRowsUnderQuantileDistribution(df: DataFrame, columnName: String, sparkContext: SparkContext, computationDetails: Boolean) : DataFrame = {
    // Compute distribution of "columnName" values (for each of them how many times it occurs in distinct rating)
    val ratingDistribution: Seq[(Int, Int)] = df.select(columnName).rdd
      .map(r => (r(0).toString.toInt, 1)).reduceByKey((n1, n2) => n1 + n2).sortBy(_._2).collect()
    // Compute first distribution quantile in order to remove "columnName" values below
    val distrLen = ratingDistribution.length
    val firstDistributionQuantile = ratingDistribution(distrLen/4)._2
    val ratingDistributionRDD = sparkContext.parallelize(ratingDistribution)
      .filter(_._2 > firstDistributionQuantile)
    // Keep DataFrame rows with column values with number of ratings greater than "firstDistributionQuantile"
    val columnValuesToKeepIndices = ratingDistributionRDD.map(_._1).collect()
    val newDf = df.filter(df(columnName).cast(IntegerType).isInCollection(columnValuesToKeepIndices))
    // Print some details of the preparation phase
    if (computationDetails) {
      val valuesDifference = df.except(newDf).select(columnName).rdd.map(r => r(0).toString.toInt).distinct().collect()
      val detailsToPrint = " --- Attribute " + columnName + " Distribution Preparation --- " +
        "\n1st Distr Quantile: " + firstDistributionQuantile +
        "\nStarting Dataframe rows: " + df.count() + " -> New Dataframe rows: " + newDf.count() +
        "\n" + valuesDifference.mkString("Attribute values removed (" + columnName + "): [",",","]")
      println(detailsToPrint)
    }
    newDf
  }

  // Drop the ratings DataFrame rows about users with no ratings equal to 4 or 5
  def removeUsersWithNo4or5Ratings(df: DataFrame, computationDetails: Boolean): DataFrame = {
    // Compute sequence of users with at least one 4 or 5 rating
    val highRatings = List(4, 5)
    val usersToKeep: Seq[Int] = df.filter(df("rating").cast(IntegerType).isInCollection(highRatings))
      .select("userId").rdd.map(_(0).toString.toInt).distinct().collect()
    // Compute DataFrame with only rows related to users with 4 or 5 ratings
    val newDf = df.filter(df("userId").cast(IntegerType).isInCollection(usersToKeep))
    if (computationDetails) {
      val usersDifference = df.except(newDf).select("userId").rdd.map(_(0).toString.toInt).distinct().collect()
      val detailsToPrint = " --- Users with no 4 or 5 ratings Remotion ---" +
        "\nStarting Dataframe rows : " + df.count() + " -> New Dataframe rows: " + newDf.count() +
        "\n" + usersDifference.mkString("Attribute values removed (userId): [",",","]")
      println(detailsToPrint)
    }
    newDf
  }

  // Prepare the rating DataFrame in order to compute the movie recommendations
  def ratingsDataFramePreparation(ratDf: DataFrame, sparkContext: SparkContext, computationDetails: Boolean = false): DataFrame = {
    // Drop the rows containing any null or NaN values
    var df = ratDf.na.drop()
    // Remove movies and users with a low amount of reviews
    df = removeRowsUnderQuantileDistribution(df, "movieId", sparkContext, computationDetails)
    df = removeRowsUnderQuantileDistribution(df, "userId", sparkContext, computationDetails)
    // Remove users with no 4 or 5 rating
    df = removeUsersWithNo4or5Ratings(df, computationDetails)
    // Return prepared DataFrame
    df
  }

  // Compute cosine similarity between 2 arrays of the same length
  def cosineSimilarity(arrayOne: Array[Float], arrayTwo: Array[Float]): Float = {
    if (arrayOne.length != arrayTwo.length)
      throw new Exception("Error in Cosine Similarity: Arrays with Different Lengths")
    // Compute cosine similarity
    val numRes = (arrayOne zip arrayTwo).map(c => c._1 * c._2).sum
    val denomRes = math.sqrt(arrayOne.map(math.pow(_, 2)).sum) * math.sqrt(arrayTwo.map(math.pow(_, 2)).sum)
    var cosRes = numRes
    if (denomRes != 0)
      cosRes /= denomRes.toFloat
    cosRes
  }

  // Compute sparse dot product between 2 iterables containing tuples in the form of (position, value)
  def sparseDotProduct(one: Iterable[(Int, Float)], two: Iterable[(Int, Float)]) = {
    val mapTwo = two.toMap.withDefaultValue(0.toFloat)
    val res = one.map(
      t1 => t1._2.toFloat * mapTwo(t1._1)
    ).sum
    res
  }

  // Compute sparse cosine similarity between 2 iterables containing tuples in the form of (position, value)
  def sparseCosineSimilarity(movieRatingsOne:  Iterable[(Int, Float)], movieRatingsTwo: Iterable[(Int, Float)]): Float = {
    val numRes = sparseDotProduct(movieRatingsOne, movieRatingsTwo)
    val denRes = math.sqrt(movieRatingsOne.map(t => math.pow(t._2, 2)).sum) * math.sqrt(movieRatingsTwo.map(t => math.pow(t._2, 2)).sum)
    if (denRes == 0)
      return 0.toFloat
    (numRes/denRes).toFloat
  }

  // Display recommendation results
  def displayRecommendations(recommenderSystemType: String, recommendationArray: RDD[(Int, Seq[(String, Float)])], recommendationsNumber: Int, numberUsers: Int): String = {
    var strToPrint = recommenderSystemType + "\n"
    val arr = recommendationArray.takeOrdered(numberUsers)(Ordering[Int].on(x => x._1))
      .map(tuple => (tuple._1, tuple._2.sortWith(_._2 > _._2).take(recommendationsNumber)))
    for (i <- arr.indices) {
      strToPrint += "User: " + arr(i)._1 + " -> "
      for (j <- arr(i)._2.indices) {
        strToPrint += "\t" + (j+1) + ")'" + arr(i)._2(j)._1 + "' score: " + arr(i)._2(j)._2 + "/5"
        if (j < arr(i)._2.length-1)
          strToPrint += ";"
      }
      strToPrint += "\n"
    }
    strToPrint+"\n"
  }
}
