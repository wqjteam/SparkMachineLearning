package com.wqj.app.learn2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Auther: wqj
  * @Date: 2018/12/11 17:07
  * @Description:
  */
object testCorrect {
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
    val rdd = sc.textFile("src/files/learn/c").map(_.split(" ").map(_.toDouble))
    val rddx = rdd.filter(_ (0) == 1).flatMap(x => {
      x.map(x => {
        x
      })
    })
    val rddy = rdd.filter(_ (1) == 2).flatMap(x => {
      x.map(x => {
        x
      })
    })
    val correct1: Double = Statistics.corr(rddx, rddy) //皮尔逊
    val correct2: Double = Statistics.corr(rddx, rddy,"spearman") //斯皮尔曼
    println(correct1)  //皮尔逊
    println(correct2)  //斯皮尔曼
  }
}
