package live.analyse

import com.alibaba.fastjson.JSON
import live.utils.DateUtils
import offline.utils.JedisUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * flume以Taildir Source为源将数据迁移至kafka
  * 以直流从kafka中读取关锁日志
  * sparkStreaming分析数据实时写入redis并更新数据
  */
object unLockStreaming {
  def main(args: Array[String]): Unit = {

    //主题和消费组
    val group="sh001"
    val topic="bike_unlock"

    //创建sparkstreaming
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[*]")
    val streamingContext = new StreamingContext(sparkConf,Seconds(5))

    //配置kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop1:9092,hadoop2:9092,hadoop3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest", // lastest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic)
    //在Kafka中记录读取偏移量
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      //位置策略
      PreferConsistent,
      //订阅的策略
      Subscribe[String, String](topics, kafkaParams)
    )


    //迭代DStream
    stream.foreachRDD { rdd =>
      //获取该RDD对于的偏移量
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      //处理数据
//      rdd.foreach{ line =>
//        println(line.key() + " " + line.value())
//      }
      val baseData = rdd.map(t => JSON.parseObject(t.value()))
        .map(jsonObj => {
          val province=jsonObj.getString("province")
          val city=jsonObj.getString("city")
          val client=jsonObj.getString("client")
          val deviceType=jsonObj.getString("deviceType")
          val unlock=jsonObj.getString("unlock")
          val distance=jsonObj.getDouble("distance")
          val hour=DateUtils.caculateHour(jsonObj.getLong("btimestamp"))
          val date=DateUtils.caculateDate(jsonObj.getLong("btimestamp"))
          val time=DateUtils.caculateTime(jsonObj.getLong("btimestamp"),jsonObj.getLong("etimestamp"))
          ("bike"+date,province,city,client,deviceType,unlock,distance,time,hour)
        })

      /**
        *总的骑行次数、骑行距离、骑行时间
        */
      baseData.map( t =>(t._1,List[Double](t._7,t._8)))
        .reduceByKey((a,b) =>{
          ( a zip b) map (x => x._1+x._2)
        }).foreachPartition(itr =>{
        val client = JedisUtils.getJedisClient
        itr.foreach(tp =>{
          client.hincrBy(tp._1,"total",1)
          client.hincrByFloat(tp._1,"distance",tp._2.head)
          client.hincrByFloat(tp._1,"time",tp._2(1))

          client.expire(tp._1, 60 * 60 * 24 * 2)
        })
        client.close()
      })

      /**
        * 每小时的骑行次数、骑行距离、骑行时间
        */
      baseData.map( t =>(t._1+"+hour+"+t._9,List[Double](t._7,t._8)))
        .reduceByKey((a,b) =>{
          ( a zip b) map (x => x._1+x._2)
        }).foreachPartition(itr =>{
        val client = JedisUtils.getJedisClient
        itr.foreach(tp =>{
          client.hincrBy(tp._1,"total",1)
          client.hincrByFloat(tp._1,"distance",tp._2.head)
          client.hincrByFloat(tp._1,"time",tp._2(1))

          client.expire(tp._1, 60 * 60 * 24 * 2)
        })
        client.close()
      })

      /**
        * 每个省市的骑行数据
        */
      baseData.map( t =>(t._1+"+"+t._2+t._3,List[Double](t._7,t._8)))
        .reduceByKey((a,b) =>{
          ( a zip b) map (x => x._1+x._2)
        }).foreachPartition(itr =>{
        val client = JedisUtils.getJedisClient
        itr.foreach(tp =>{
          client.hincrBy(tp._1,"total",1)
          client.hincrByFloat(tp._1,"distance",tp._2.head)
          client.hincrByFloat(tp._1,"time",tp._2(1))

          client.expire(tp._1, 60 * 60 * 24 * 2)
        })
        client.close()
      })


      //更新偏移量
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }


    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
