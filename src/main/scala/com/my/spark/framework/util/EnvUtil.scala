package com.my.spark.framework.util

import org.apache.spark.SparkContext

object EnvUtil {

  private val ssLocal = new ThreadLocal[SparkContext]

  def put(sc:SparkContext): Unit ={
    ssLocal.set(sc)
  }

  def take(): SparkContext ={
    ssLocal.get()
  }

  def clear(): Unit ={
    ssLocal.remove()
  }

}
