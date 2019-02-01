package offline.analyse.recharge

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  *根据省市日期计算支付指标
  * 1 支付总数
  * 2 支付的客户端类型
  * 3 支付的设备类型
  * 4 支付的网络类型
  * 5 支付的支付类型
  * 6 支付押金的总次数、总金额、支付方式
  * 7 支付充值的总次数、总金额、支付方式
  */
object ProCityRechargeRpt {
  def main(args: Array[String]): Unit = {
    if(args.length!=1){
      println(
        """
          |参数：
          | 输入路径 |  hdfs://hadoop1:9000/bike/washed/Recharge
        """.stripMargin)
      sys.exit()
    }
    val Array(logInputPath)=args

    //    设置spark上下文的名称、运行模式、序列化方式
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[*]")
      .set("spark.sql.session.timeZone", "UTC")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //    创建spark上下文
    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // 读取parquet文件
    val parquetData: DataFrame=session.read.parquet(logInputPath)
    parquetData.createTempView("log")

    // sparkSql处理数据
    val result1=session.sql("select date,province,city,COUNT(*) ordersum " +
      "from log " +
      "group by date,province,city ")
    val result2=session.sql("select date,province,city,COUNT(*) clientsum,client " +
      "from log " +
      "group by date,province,city,client ")
    val result3=session.sql("select date,province,city,COUNT(*) deviceTypeSum,deviceType " +
      "from log " +
      "group by date,province,city,deviceType ")
    val result4=session.sql("select date,province,city,COUNT(*) networksum,networkingmannerid " +
      "from log " +
      "group by date,province,city,networkingmannerid ")
    val result5=session.sql("select date,province,city,COUNT(*) rechargeSum,rechargetype " +
      "from log " +
      "group by date,province,city,rechargetype ")
    val result6=session.sql("select date,province,city,rechargesource,count(*) cashNum,SUM(amount) amount  " +
      "from log " +
      "where  rechargetype=1 " +
      "group by date,province,city,rechargesource ")
    val result7=session.sql("select date,province,city,rechargesource,COUNT(*) rechargeNum,SUM(amount) amount " +
      "from log " +
      "where  rechargetype=2 " +
      "group by date,province,city,rechargesource ")


    //    c存报表数据
    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    result1.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "SumPeoRecharge", props)
    result2.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "clientPeoRecharge", props)
    result3.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "devicePeoRecharge", props)
    result4.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "networkPeoRecharge", props)
    result5.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "rechargeTypePeoRecharge", props)
    result6.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "cashPeoRecharge", props)
    result7.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "rechargePeoRecharge", props)


    session.close()
  }
}
