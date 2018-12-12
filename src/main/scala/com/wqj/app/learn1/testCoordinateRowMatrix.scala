package com.wqj.app.learn1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.SparkSession

/**
  * @Auther: wqj
  * @Date: 2018/12/11 15:23
  * @Description:
  */
object testCoordinateRowMatrix {
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

   val rdd= sc.textFile("src/files/learn/a.txt").map(_.split(" ").map(_.toDouble))
          .map(vue2=> new MatrixEntry(vue2(0).toLong,vue2(1).toLong,vue2(2)))

    val crm = new CoordinateMatrix(rdd)

    crm.entries.foreach(println(_))
    println(crm.numCols())





  }

}
