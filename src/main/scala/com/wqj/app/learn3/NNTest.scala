package com.wqj.app.learn3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Auther: wqj
  * @Date: 2018/12/17 14:59
  * @Description:神经网络
  */
object NNTest {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("NNTest")
    sparkConf.set("spark.sql.parquet.writeLegacyFormat", "true")
    sparkConf.set("spark.debug.maxToStringFields", "300")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.sql.parquet.compression.codec", "gzip")
    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext

    val sampleRow = 1000
    val sampleColumn = 5

//    val randSamp_01=RandSampleData


  }
}
