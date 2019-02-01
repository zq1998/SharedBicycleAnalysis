package offline.analyse.bike

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 1 日活跃用户人数
  * 2 各时间段骑行的人数
  * 3 客户端版本、类型、设备型号
  */
object PeopleBikeRpt {
  def main(args: Array[String]): Unit = {
    if(args.length!=1){
      println(
        """
          |参数：
          | 输入路径 | hdfs://hadoop1:9000/bike/washed/lock | hdfs://hadoop1:9000/bike/washed/Unlock
        """.stripMargin)
      sys.exit()
    }
    val Array(logInputPath)=args

    //    设置spark上下文的名称、运行模式、序列化方式
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[*]")
//      .set("spark.sql.session.timeZone", "UTC")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //    创建spark上下文
    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // 读取parquet文件
    val parquetData: DataFrame=session.read.parquet(logInputPath)
    parquetData.createTempView("log")

    // sparkSql处理数据
    val result1=session.sql("select date,province,city,COUNT(distinct userid) usersum " +
      "from log " +
      "group by date,province,city ")
//    FROM_UNIXTIME接受bigint类型参数
    val result2=session.sql("select date,province,city,COUNT(*) sum ,FROM_UNIXTIME(btimestamp ,'H' ) hour " +
      "from log " +
      "group by date,province,city,FROM_UNIXTIME(btimestamp ,'H' ) ")
    val result3=session.sql("select date,province,city,deviceType,client,version,COUNT(*) sum " +
      "from log " +
      "group by date,province,city,version,deviceType,client ")
    //    c存报表数据
    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    result1.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "dayPeoBike", props)
    result2.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "hourPeoBike", props)
    result3.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "versionPeoBike", props)
    session.close()
  }
}
