package com.knoldus.sparkUDF

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AggregateUDF extends SparkSessionWrapper with App {

  //Registring the UDF
  val meanFunction = udaf(MeanExample)

  //loading the matches played dataset
  val matchesPlayedDF = loadingData(spark, "sampleData/matches.csv")
  matchesPlayedDF.show(5, truncate = false)

  //applying mean udf
  val meanOfWinningRunsDF = applyingMeanUDF(meanFunction, matchesPlayedDF)
  meanOfWinningRunsDF.show(truncate = false)

  /**
   * Loading the data into a dataframe
   * @param spark
   * @param dataFile
   * @return
   */
  def loadingData(spark: SparkSession, dataFile: String): DataFrame = {
    spark.read.format("csv")
      .option("header", true)
      .load(dataFile)
  }

  /**
   * Applying the User Defined Function to match df ''win_by_runs'' columns
   * @param udfFunction
   * @param matches
   * @return
   */
  def applyingMeanUDF(udfFunction: UserDefinedFunction, matches: DataFrame): DataFrame = {
    val teamWinningByRunsDF = matches.select("team1", "team2", "win_by_runs").filter(col("win_by_runs") > 0)
    teamWinningByRunsDF.select(udfFunction(col("win_by_runs")).as("the_winning_avg_runs"))
  }

}
