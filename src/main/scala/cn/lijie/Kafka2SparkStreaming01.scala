package cn.lijie

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * User: lijie
  * Date: 2017/8/4
  * Time: 15:27  
  */
object Kafka2SparkStreaming01 {

  def myFunc = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(x => {
      (x._1, x._2.sum + x._3.getOrElse(0))
    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("k2s").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    sc.setCheckpointDir("C:\\Users\\Administrator\\Desktop\\checkpointks01")
    val zk = "192.168.80.123:2181"
    val groupId = "lijieGrp"
    //key表示topic  value表示处理线程数量
    val topics = Map("lijietest001" -> 3)
    //参数ssc zk groupId topics storageLevel,注意这里的topics是不可变map 习惯用可变map发现一直报错最后才发现需要用不可变map
    //type Map[A, +B] = scala.collection.immutable.Map[A, B]
    val ds = KafkaUtils.createStream(ssc, zk, groupId, topics)
    val res = ds.map((_._2)).flatMap(_.split(" ")).map((_, 1)).updateStateByKey(myFunc, new HashPartitioner(sc.defaultParallelism), true)
    res.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
