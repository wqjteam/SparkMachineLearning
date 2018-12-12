package com.wqj.app.learn2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession

/**
  * @Auther: wqj
  * @Date: 2018/12/11 16:05
  * @Description:
  */
object testSummary {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("testRowMatrix")
    sparkConf.set("spark.sql.parquet.writeLegacyFormat", "true")
    sparkConf.set("spark.debug.maxToStringFields", "300")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.sql.parquet.compression.codec", "gzip")
    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    val rdd = sc.textFile("src/files/learn/b").map(_.split(" ").map(_.toDouble))
      .map(line => Vectors.dense(line))
    val summary = Statistics.colStats(rdd)
    println(summary.mean) //均值
    println(summary.variance) //标准差
    println(summary.normL1)  //哈曼距离 1+2+3+4
    println(summary.normL2)  //欧几里得距离

  }
}
