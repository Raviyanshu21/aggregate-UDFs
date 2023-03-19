package com.knoldus.sparkUDF

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.io.Source

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate()
  }
  spark.sparkContext.setLogLevel("ERROR")

  def getSparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    val props = new Properties()
    props.load(Source.fromFile("src/main/resources/spark.conf").bufferedReader())
    props.forEach((k, v) => sparkConf.set(k.toString, v.toString))
    sparkConf
  }

}