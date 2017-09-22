package cn.lijie.kafka

import cn.lijie.MyLog
import org.apache.log4j.Level
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * User: lijie
  * Date: 2017/8/8
  * Time: 14:04  
  */
object SparkWindowDemo {

  val myfunc = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(x => {
      (x._1, x._2.sum + x._3.getOrElse(0))
    })
  }

  def main(args: Array[String]): Unit = {
    MyLog.setLogLeavel(Level.WARN)
    val conf = new SparkConf().setMaster("local[2]").setAppName("window")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    sc.setCheckpointDir("C:\\Users\\Administrator\\Desktop\\myck01")
    val ds = ssc.socketTextStream("192.168.80.123", 9999)
    //Seconds(5)表示窗口的宽度   Seconds(3)表示多久滑动一次(滑动的时间长度)
    val re = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(20), Seconds(10))
    //    val re = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(4), Seconds(4)).updateStateByKey(myfunc, new HashPartitioner(sc.defaultParallelism), true)
    re.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
