package cn.lijie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

/**
  * User: lijie
  * Date: 2017/8/3
  * Time: 15:36
  */
object MyLog extends Logging {
  /**
    * 设置日志级别
    *
    * @param level
    */
  def setLogLeavel(level: Level): Unit = {
    val flag = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!flag) {
      logInfo("set log level ->" + level)
      Logger.getRootLogger.setLevel(level)
    }
  }
}
