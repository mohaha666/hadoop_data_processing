package com.my.spark.framework.service

import com.my.spark.framework.common.TService
import com.my.spark.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * 服务层
 */
class WordCountService extends TService{

  private val dao = new WordCountDao()
  /**
   * 数据分析
   * @return
   */
  def dataAnalysis(path:String) ={
    val data = dao.readFile(path)

    val res = data.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    res
  }

}
