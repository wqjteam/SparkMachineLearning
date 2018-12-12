package com.wqj.app.step2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Auther: wqj
  * @Date: 2018/11/27 16:33
  * @Description:
  */
object BaseModel {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("BaseModel")
    sparkConf.set("spark.sql.parquet.writeLegacyFormat", "true")
    sparkConf.set("spark.debug.maxToStringFields", "300")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.sql.parquet.compression.codec", "gzip")
    val sparksession = SparkSession.builder.config(sparkConf).getOrCreate()
    val sc = sparksession.sparkContext

    //装载训练数据
    val RatingsRDDTrain = sc.textFile("src/files/ml-100k/u1.base").map(x => {
      val fields = x.split("\t")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    })

    //装载测试数据,其中最后一列Timestamp取除10的余数作为key，Rating为值，即(Int，Rating)
    val RatingsRDDTest = sc.textFile("src/files/ml-100k/u1.test").map(line => {
      val fields = line.split("\t")
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    })

    //装载用户
    val UserRDDBefore = sc.textFile("src/files/ml-100k/u.user").map(line => {

    })

    //装载电影
    val MovieRDDBefore = sc.textFile("src/files/ml-100k/u.item").map(line => {
      val fields = line.split("\t")
      (fields(0).toInt, fields(1))
    })

    //集中训练,获取最佳的训练集
    val ranks = List(8, 12)

    val lambdas = List(0.1, 10.0)

    val numIters = List(10, 20)

    var bestModel: Option[MatrixFactorizationModel] = None

    var bestValidationRmse = Double.MaxValue

    var bestRank = 0

    var bestLambda = -1.0

    var bestNumIter = -1

//    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
//
//      val model = ALS.train(RatingsRDDTrain, rank, numIter, lambda)
//
//      val validationRmse = computeRmse(model, validation, numValidation)
//
//      println("RMSE(validation) = " + validationRmse + " for the model trained with rank = "
//
//        + rank + ",lambda = " + lambda + ",and numIter = " + numIter + ".")
//
//
//
//      if (validationRmse < bestValidationRmse) {
//
//        bestModel = Some(model)
//
//        bestValidationRmse = validationRmse
//
//        bestRank = rank
//
//        bestLambda = lambda
//
//        bestNumIter = numIter
//
//      }
//
//    }
//
//
//
//    //用最佳模型预测测试集的评分，并计算和实际评分之间的均方根误差（RMSE）
//
//    val testRmse = computeRmse(bestModel.get, test, numTest)
//
//    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
//
//      + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")
//
//
//
//    //create a naive baseline and compare it with the best model
//
//    val meanRating = training.union(validation).map(_.rating).mean
//
//    val baselineRmse = math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).reduce(_ + _) / numTest)
//
//    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
//
//    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")
//
//
//
//    //推荐前十部最感兴趣的电影，注意要剔除用户已经评分的电影
//
//    val myRatedMovieIds = myRatings.map(_.product).toSet
//
//    val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
//
//    val recommendations = bestModel.get
//
//      .predict(candidates.map((0, _)))
//
//      .collect
//
//      .sortBy(-_.rating)
//
//      .take(10)
//
//    var i = 1
//
//    println("Movies recommended for you:")
//
//    recommendations.foreach { r =>
//
//      println("%2d".format(i) + ": " + movies(r.product))
//
//      i += 1
//
//    }
//
//
//
//    sc.stop()
//
//
//  }
//
//
//  /** 校验集预测数据和实际数据之间的均方根误差 **/
//
//  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
//
//
//    val predictions: RDD[Rating] = model.predict((data.map(x => (x.user, x.product))))
//
//    val predictionsAndRatings = predictions.map { x =>
//      ((x.user, x.product), x.rating)
//    }
//      .join(
//        data.map(x => ((x.user, x.product), x.rating))
//      ).values
//
//    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)

  }


}
