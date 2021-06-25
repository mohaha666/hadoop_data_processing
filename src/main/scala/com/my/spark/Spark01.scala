package com.my.spark

import org.apache.spark.{SparkConf, SparkContext}

object Spark01 {
  def main(args: Array[String]): Unit = {
    // application如何连接spark框架
    // todo 建立和spark框架的连接
    val conf = new SparkConf().setMaster("local[*]").setAppName("MySpark")
    val sc = new SparkContext(conf)
    // todo 执行业务操作
    val dataRdd = sc.textFile("C:\\Users\\jiamo\\Documents\\jm\\tmp\\spark\\access.log")


    val mapRdd = dataRdd.flatMap(
      line => {
        val content:Array[String] = line.split(" ")
        val url = content(6)
        if (url.contains("user") && url.contains("fans")) {
          List("fans", (1, 0, 0, 0))
        } else if (url.contains("user") && url.contains("activities")) {
          List("activities", (0, 1, 0, 0))
        } else if (url.contains("user") && url.contains("topic")) {
          List("topic", (0, 0, 1, 0))
        }else{
          List("other", (0, 0, 0, 1))
        }
      }
    )



    mapRdd.take(10).foreach(println)


    // todo 关闭连接
    sc.stop()
  }

  /**
   * 自定义累加器
   * 1. 继承AccumulatorV2，定义泛型
   */
//  class HotCategoryAccumulator extends AccumulatorV2[]{
//
//  }
}
