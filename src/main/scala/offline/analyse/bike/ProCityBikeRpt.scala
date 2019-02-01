package offline.analyse.bike

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  *根据省市日期计算指标
  * 将报表数据存入mysql中
  * 1 总使用次数
  * 2 设备类型使用次数
  * 3 客户端类型使用次数
  * 4 解锁方式次数
  * 5 使用车的个数
  * 6 客户端网络类型
  * 7 总里程数
  */
object ProCityBikeRpt {
  def main(args: Array[String]): Unit = {
    if(args.length!=1){
      println(
        """
          |参数：
          | 输入路径 |  hdfs://hadoop1:9000/bike/washed/lock | hdfs://hadoop1:9000/bike/washed/Unlock
        """.stripMargin)
      sys.exit()
    }
    val Array(logInputPath)=args

    //    设置spark上下文的名称、运行模式、序列化方式
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //    创建spark上下文
    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // 读取parquet文件
    val parquetData: DataFrame=session.read.parquet(logInputPath)
    parquetData.createTempView("log")

//    sparkSql处理数据
    val result1=session.sql("select date,province,city,count(*) sum " +
      "from log " +
      "group by date,province,city")
    val result2=session.sql("select date,province,city,deviceType,count(*) sum " +
      "from log " +
      "group by date,province,city,deviceType")
    val result3=session.sql("select date,province,city,client,count(*) sum " +
      "from log " +
      "group by date,province,city,client")
    val result4=session.sql("select date,province,city,unlock,count(*) sum " +
      "from log " +
      "group by date,province,city,unlock")
    val result5=session.sql("select date,province,city,COUNT(distinct bikeid) sumbike " +
      "from log " +
      "group by date,province,city")
    val result6=session.sql("select date,province,city,networkingmannerid,count(*) sum " +
      "from log " +
      "group by date,province,city,networkingmannerid")
    val result7=session.sql("select date,province,city,SUM(distance) sumdistance " +
      "from log " +
      "group by date,province,city")

//    c存报表数据
    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    result1.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "ProCityAreaBike", props)
    result2.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "DeviceAreaBike", props)
    result3.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "ClientAreaBike", props)
    result4.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "UnlockAreaBike", props)
    result5.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "SumbikeAreaBike", props)
    result6.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "NetworkAreaBike", props)
    result7.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "DistanceAreaBike", props)


    session.close
  }


}
