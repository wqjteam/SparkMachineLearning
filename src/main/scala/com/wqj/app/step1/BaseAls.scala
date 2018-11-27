package com.wqj.app.step1

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession

/**
  * @Auther: wqj
  * @Date: 2018/11/26 17:01
  * @Description:
  */
object BaseAls {
  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("BaseAls")
    sparkConf.set("spark.sql.parquet.writeLegacyFormat", "true")
    sparkConf.set("spark.debug.maxToStringFields", "300")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.sql.parquet.compression.codec", "gzip")
    //  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //  sparkConf.set("spark.kryoserializer.buffer", "128")
    val sparksession = SparkSession.builder
      .config(sparkConf)
      //      .enableHiveSupport()
      .getOrCreate()

    //创建训练集
    //数据原型 196 242 3
    val rawData = sparksession.sparkContext.textFile("ml-100k/u.data")
    rawData.first()
    val rawRatings = rawData.map(_.split("\t").take(3))
    rawRatings.first()
    //这是因为MLlib的ALS推荐系统算法包只支持Rating格式的数据集
    val ratings = rawRatings.map { case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }


    //训练模型
    //- rank：对应的是隐因子的个数，这个值设置越高越准，但是也会产生更多的计算量。一般将这个值设置为10-200；
    //- iterations：对应迭代次数，一般设置个10就够了；
    //- lambda：该参数控制正则化过程，其值越高，正则化程度就越深。一般设置为0.01
    // 启动ALS矩阵分解
    val model = ALS.train(ratings, 50, 10, 0.01)
    model.userFeatures.count

    //用789预测123这个商品
    val predictdRating = model.predict(789, 123)

    //对789 推荐前10个商品
    val userId = 789
    val K = 10
    val topKRecs = model.recommendProducts(userId, K)
    println(topKRecs.mkString("\n"))
  }
}
