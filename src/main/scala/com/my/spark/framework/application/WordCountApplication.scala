package com.my.spark.framework.application

import com.my.spark.framework.common.TApplication
import com.my.spark.framework.controller.WordCountController

object WordCountApplication extends App with TApplication {

  start (){
    val controller = new WordCountController()

    val path = "C:\\Users\\jiamo\\Documents\\jm\\tmp\\spark\\word.txt"

    controller.dispatch(path)
  }
}
