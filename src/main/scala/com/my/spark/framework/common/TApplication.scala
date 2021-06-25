package com.my.spark.framework.common

import com.my.spark.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {
  def start(master:String="local[*]", app:String="Application")(op : => Unit): Unit ={
    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(conf)

    EnvUtil.put(sc)

    try{
      op
    } catch {
      case ex => println(ex.getMessage)
    }

    sc.stop()
    EnvUtil.clear()
  }

}
