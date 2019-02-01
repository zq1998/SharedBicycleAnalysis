package offline.analyse.tags

import ch.hsr.geohash.GeoHash
import offline.utils.{BaiduGeoApi, JedisUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import redis.clients.jedis.JedisPool


/**
  * 获取日志中的经纬度，请求百度API，获取商圈信息
  * 将经纬度GeoHash编码，将标签与GeoHashCode存入redis
  * 构建知识库
  */
object LatLong2Business {
  def main(args: Array[String]): Unit = {
    if(args.length!=1){
      println(
        """
          |参数：
          | 输入路径  |  hdfs://hadoop1:9000/bike/washed/Recharge | hdfs://hadoop1:9000/bike/washed/Unlock
        """.stripMargin)
      sys.exit()
    }
    val Array(logInputPath)=args

    //    设置spark上下文的名称、运行模式、序列化方式
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.session.timeZone", "UTC")

    //    创建spark上下文
    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // 读取parquet文件
    import session.implicits._
    session.read.parquet(logInputPath)
        .select("latitude","longitude")
        .where("latitude > 3 and latitude < 54 and longitude > 73 and longitude < 136")
        .distinct()
        .foreachPartition(itr =>{
          val jedis = JedisUtils.getJedisClient
          itr.foreach(row =>{
            val lat = row.getAs[Double]("latitude")
            val long= row.getAs[Double]("longitude")

            val geoHashCode = GeoHash.withCharacterPrecision(lat,long,8).toBase32
            val business = BaiduGeoApi.getBusiness(lat+","+long)

            jedis.set(geoHashCode,business)
          })
          jedis.close()
        })

    session.close()
  }

}
