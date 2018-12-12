package com.wqj.app.learn1

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * @Auther: wqj
  * @Date: 2018/12/11 14:27
  * @Description:
  */
object testLabeledPoint {

  def main(args: Array[String]): Unit = {
    val vd :linalg.Vector=Vectors.dense(2,4,6)
    val pos = LabeledPoint(1,vd)    //对密集向量简历标记点
    println(pos.features)
    println(pos.label)


    val vs: linalg.Vector = Vectors.sparse(4,Array(0,1,2),Array(2, 0, 6))
    val neg = LabeledPoint(2,vs)
    println(neg.features)
    println(neg.label)
  }
}
