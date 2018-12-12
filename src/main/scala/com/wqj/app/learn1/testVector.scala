package com.wqj.app.learn1

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors

/**
  * @Auther: wqj
  * @Date: 2018/12/11 14:07
  * @Description:
  */
object testVector {
  def main(args: Array[String]): Unit = {

    //密集举证
    val vd: linalg.Vector = Vectors.dense(2, 0, 6)
    println(vd(2))

    //Array(0,1,2) 是Array(2, 0, 6)向量的下标 4必须大于Array(2, 0, 6)的个数
    //不支持浮点型,支持支整数
    val vs: linalg.Vector = Vectors.sparse(4,Array(0,1,2),Array(2, 0, 6))
    println(vs(0))

  }
}
