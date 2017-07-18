package KafkaStreaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.json4s.jackson.Serialization.write
import org.json4s._
import org.json4s.jackson.Serialization



object KafkaTest {
  implicit val formats = DefaultFormats//数据格式化时需要

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // 创建ssc连接 一秒钟更新一次
    val ssc = new StreamingContext("local[*]", "KafkaExample", Seconds(1))
    ssc.checkpoint(".")
    //kafka服务器的hostname：port ，不是zookeeper的
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "example",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //你想要监听kafka的topic列表
    val topics = List("test","test2","my-replicated-topic").toSet
    /* Create our Kafka stream, which will contain (topic,message) pairs. We tack a
       map(_._2) at the end in order to only get the messages, which contain individual
       lines of data.*/
    val lines = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ).map(_.value());

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_+_,_-_, Seconds(1), Seconds(1), 1).foreachRDD(rdd => {
      if(rdd.count !=0 ){
        val props = new HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        // 实例化一个Kafka生产者
        val producer = new KafkaProducer[String, String](props)
        // rdd.colect即将rdd中数据转化为数组，然后write函数将rdd内容转化为json格式
        val str = write(rdd.collect)

        // 封装成Kafka消息，topic为"result"
        val message = new ProducerRecord[String, String]("result", null, str)
        // 给Kafka发送消息
        producer.send(message)
      }
    })

    // 开始计算
    ssc.start()
    ssc.awaitTermination()
  }
}