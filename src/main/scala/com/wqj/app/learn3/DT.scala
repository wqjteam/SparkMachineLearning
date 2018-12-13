package com.wqj.app.learn3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

/**
  * @Auther: wqj
  * @Date: 2018/12/13 15:37
  * @Description:
  */
object DT {

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

    //    val trainingData = sc.textFile("src/files/learn/dt").map(line => {
    //      val strs = line.split(" ")
    //      new LabeledPoint(strs(0).toDouble, Vectors.dense(Array(strs(1).toDouble, strs(2).toDouble, strs(3).toDouble)))
    //    }
    //    )

    val data = MLUtils.loadLibSVMFile(sc, "src/files/learn/dt")
    val splits = data.randomSplit(Array(0.5, 0.5))
    val (trainingData, testData) = (splits(0), splits(1))
    val numClasses = 2 // 分为两类
    val categoricalFeaturesInfo = Map[Int, Int]() //设定输入格式
    val Inpurity = "entropy" //设定计算增益方式
    val maxDepth = 5 // 设定输的高度
    val maxBins = 3 //设定分裂数据集

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo
      , Inpurity, maxDepth, maxBins)
    println(model.topNode)


    //验证数据
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println(s"Test Error = $testErr")
    println(s"Learned classification tree model:\n ${model.toDebugString}")

    // Save and load model
//    model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
//    val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")
  }
}
