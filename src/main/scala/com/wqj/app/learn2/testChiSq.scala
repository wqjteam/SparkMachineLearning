package com.wqj.app.learn2

import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.mllib.stat.Statistics

/**
  * @Auther: wqj
  * @Date: 2018/12/12 14:20
  * @Description:
  */
object testChiSq {
  def main(args: Array[String]): Unit = {
    val vd = Vectors.dense(1, 2, 3, 4, 5)
    val vdResult = Statistics.chiSqTest(vd)
    println(vdResult)
    println("-----------------------------------")
    val mtx = Matrices.dense(3, 2, Array(1, 2, 5, 2, 4, 6))
    val mtxResult = Statistics.chiSqTest(mtx)
    println(mtxResult)
  }

  //自由度 :总体参数估计量中独立自由变量的数目
  //统计量 : 不同方法的统计量
  //p值    : 显著性差异的指标
  //方法   : 卡房检验使用的方法

  // p<0.05  被认为不存在显著的差异


}
