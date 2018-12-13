package com.wqj.app.learn2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession

/**
  * @Auther: wqj
  * @Date: 2018/12/12 15:56
  * @Description:
  */
object CollaborativeFilter {
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

    val ratings = sc.textFile("src/files/ml-100k/u.data")
      .map(x => {
        val aa = x.split("\t")
        Rating(aa(0).toInt, aa(1).toInt, aa(2).toDouble)

      })

    val rank = 2 //设置影藏因子
    val numIterations = 2 //设置迭代次数
    val model = ALS.train(ratings, rank, numIterations, 0.01)


    var rs = model.recommendProducts(2, 1)

    rs.foreach(println(_))
  }
}
