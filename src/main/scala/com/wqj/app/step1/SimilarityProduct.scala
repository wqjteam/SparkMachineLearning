package com.wqj.app.step1

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

/**
  * @Auther: wqj
  * @Date: 2018/11/26 17:50
  * @Description:
  */
object SimilarityProduct {

  def main(args: Array[String]): Unit = {


    val conf = new Configuration
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("SimilarityProduct")
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
    //接下来获取物品(本例以物品567为例)的因子特征向量，并将它转换为jblas的矩阵格式

    // 选定id为567的电影
    val itemId = 567
    // 获取该物品的隐因子向量
    val itemFactor = model.productFeatures.lookup(itemId).head
    // 将该向量转换为jblas矩阵类型
    val itemVector = new DoubleMatrix(itemFactor)


    //计算物品567和所有其他物品的相似度：
    // 计算电影567与其他电影的相似度
    val sims = model.productFeatures.map { case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }

    val K = 10
    // 获取与电影567最相似的10部电影
    val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    // 打印结果
    println(sortedSims.mkString("\n"))

    //查看结果
    // 打印电影567的影片名
    //    println(titles(567))
    // 获取和电影567最相似的11部电影(含567自己)
    val sortedSims2 = sims.top(K + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    // 再打印和电影567最相似的10部电影
    //    sortedSims2.slice(1, 11).map { case (id, sim) => (titles(id), sim) }.mkString("\n")


  }


  // 定义相似度函数
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
}
