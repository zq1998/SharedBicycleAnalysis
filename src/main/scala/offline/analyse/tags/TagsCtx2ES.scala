package offline.analyse.tags

import live.utils.JedisUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

/**
  * 将标签信息放入ElasticSearch
  * 用于根据标签信息查找对应用户
  * 标签信息带有时间戳，标签会随着时间戳而衰减
  */
object TagsCtx2ES {
  def main(args: Array[String]): Unit = {
    if(args.length!=1){
      println(
        """
          |参数：
          | 输入路径  |  hdfs://hadoop1:9000/bike/washed/lock | hdfs://hadoop1:9000/bike/washed/Unlock
        """.stripMargin)
      sys.exit()
    }
    val Array(logInputPath)=args

    //    设置spark上下文的名称、运行模式、序列化方式
    //    ES的http端口为9200，java的tcp端口为9300
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[*]")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "hadoop1,hadoop2,hadoop3")
      .set("es.port", "9200")


    //    创建spark上下文
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    //    生成标签
    import spark.implicits._
    var info =""
    spark.read.parquet(logInputPath).where(
      """
          userid !=""
      """.stripMargin
    ).rdd.mapPartitions(par => {
      val jedis = JedisUtils.getJedisClient
      val result = par.map(row => {
        val userid = row.getAs[String]("userid")

        val clients = Tags4Client.makeTags(row)
        val bikes = Tags4BIke.makeTags(row)
        val business = Tags4Business.makeTags(row, jedis)
        (userid, (clients ++ bikes ++ business).toList)
      })
      jedis.close()
      result
    }).reduceByKey((a, b) => {
      //      (a._2 ++ b._2).groupBy(_._1).map{
      //        case (k,sameTags) =>(k,sameTags.map(_._2).sum)
      //      }.toList
      (a ++ b).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList
    }).map{
      case (a ,b) =>
        info=a
        b.toMap ++ Map("id" -> a) ++ Map("timestamp" -> System.currentTimeMillis())
    }.saveToEs("sharedbicycle/aaa")


    spark.close()

  }
}
