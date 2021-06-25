package com.my.spark.framework.controller

import com.my.spark.framework.common.TController
import com.my.spark.framework.service.WordCountService

/**
 * 控制层
 */
class WordCountController extends TController{

  private val service = new WordCountService()

  /**
   * 调度
   */
  def dispatch(path:String): Unit ={
    val res = service.dataAnalysis(path)

    res.collect().foreach(println)
  }

}
