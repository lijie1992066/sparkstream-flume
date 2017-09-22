package cn.lijie

import java.net.InetSocketAddress

import org.apache.log4j.Level
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * User: lijie
  * Date: 2017/8/3
  * Time: 15:19  
  */
object Flume2SparkStreaming02 {

  def myFunc = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(x => {
      (x._1, x._2.sum + x._3.getOrElse(0))
    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("fs01").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val addrs = Seq(new InetSocketAddress("192.168.80.123", 10086))
    val ds = FlumeUtils.createPollingStream(ssc, addrs, StorageLevel.MEMORY_AND_DISK_2)
    sc.setCheckpointDir("C:\\Users\\Administrator\\Desktop\\checkpointt")
    val res = ds.flatMap(x => {
      new String(x.event.getBody.array()).split(" ")
    }).map((_, 1)).updateStateByKey(myFunc, new HashPartitioner(sc.defaultParallelism), true)
    res.print()
    ssc.start()
    ssc.awaitTermination()
  }
}