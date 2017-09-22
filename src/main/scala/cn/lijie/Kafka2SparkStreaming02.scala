import cn.lijie.kafka.KafkaManager
import kafka.serializer.StringDecoder
import org.apache.log4j.Level
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Kafka2SparkStreaming02 {

  def myFunc = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(x => {
      (x._1, x._2.sum + x._3.getOrElse(0))
    })
  }

  def main(args: Array[String]) {
    val brokers = "192.168.80.123:9092"
    val topics = "lijietest001"
    val groupId = "groupId"
    val sparkConf = new SparkConf().setAppName("Direct").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(sc, Seconds(5))
    sc.setCheckpointDir("C:\\Users\\Administrator\\Desktop\\checkpointks02")
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )
    val km = new KafkaManager(kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val ds = messages.map((_._2)).flatMap(_.split(" ")).map((_, 1)).updateStateByKey(myFunc, new HashPartitioner(sc.defaultParallelism), true)
    //保存offset
    messages.foreachRDD(rdd => {
      km.updateZKOffsets(rdd)
    })
    ds.print()
    ssc.start()
    ssc.awaitTermination()
  }
}  