package com.wqj.app.learn2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Auther: wqj
  * @Date: 2018/12/11 17:43
  * @Description:
  */
object testStratifySampling {

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

   val rdd= sc.parallelize(Array("aa", "bb", "cc", "ddd", "eee", "fff", "ee")).map(row => {
      if (row.length == 3) {
        (row, 1.toDouble)
      } else {
        (row, 2.toDouble)
      }
    })

    //设定抽样格式
    val fractions = Map("aa" -> 2.toDouble)

    val approxSample = rdd.sampleByKey(false,fractions,0)

     approxSample.foreach(println(_))

  }
}
