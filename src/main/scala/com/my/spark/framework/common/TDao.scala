package com.my.spark.framework.common

import com.my.spark.framework.util.EnvUtil

trait TDao {
  def readFile(path:String) ={
    val sc = EnvUtil.take()
    sc.textFile(path).cache()
  }
}
