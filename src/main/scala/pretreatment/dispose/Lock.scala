package pretreatment.dispose

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONObject}
import offline.utils.BaiduGeoApi
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import pretreatment.SchemaUtils.LockSchema

/**
  * 对开锁日志进行预处理并清洗
  * 转换为parquet文件格式，采用snappy压缩格式
  */
object Lock {
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      println(
        """
          |参数：
          | logInputPath | hdfs://hadoop1:9000/bike/unwashed/lock
          | compressionCode <snappy, gzip, lzo> | snappy
          | resultOutputPath | hdfs://hadoop1:9000/bike/washed/lock
        """.stripMargin
      )
      sys.exit()
    }

    val Array(logInputPath, compressionCode,resultOutputPath)=args

//    设置spark上下文的名称、运行模式、序列化方式、压缩编码
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
              .setMaster("local[*]")
              .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
              .set("spark.sql.parquet.compression.codec", compressionCode)

//    创建spark上下文
    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

//    读入数据
    val rawdata=session.sparkContext.textFile(logInputPath)

//    处理数据
    val disposedData = rawdata
      .map(line => {
        val value = JSON.parse(line).asInstanceOf[JSONObject]
        val locationObj = value.get("location").asInstanceOf[JSONObject]
        Row(
          value.getString("sessionid"),
          value.getString("userid"),
          value.getString("bikeid"),
          value.getTimestamp("timestamp"),
          value.getString("date"),
          value.getString("province"),
          value.getString("city"),
          value.getString("region"),
          locationObj.getDouble("latitude"),
          locationObj.getDouble("longitude"),
          value.getString("ip"),
          value.getInteger("client"),
          value.getString("mac"),
          value.getInteger("deviceType"),
          value.getInteger("unlock"),
          value.getInteger("networkingmannerid"),
          value.getString("version")
        )
      })
    val dataFrame=session.createDataFrame(disposedData,LockSchema.logStructType)

    //输出数据
    dataFrame.write.parquet(resultOutputPath)
    session.stop()

  }

}
