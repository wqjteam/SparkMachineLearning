package com.wqj.app.learn1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.sql.SparkSession

/**
  * @Auther: wqj
  * @Date: 2018/12/11 14:43
  * @Description:
  */
object testRowMatrix {

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
    val baseRdd =sc.parallelize(Array(Array(1, 2, 3), Array(4, 5, 6)))
//    baseRdd.foreach(x=>{
//      println(x.toBuffer)
//    })
    val resultRdd  = baseRdd.map(line=>{
      Vectors.dense(line.map(_.toDouble))
    }).map(vector=>{
      //简历索引
      new IndexedRow(vector.size,vector)
    })

    //val rm = new RowMatrix(resultRdd)
    val rm = new IndexedRowMatrix(resultRdd)
    //懒加载
    println(rm.getClass)
    println(rm.rows.foreach(x=>print(x)))
    println(rm.numRows())
    println(rm.numCols())
  }
}
