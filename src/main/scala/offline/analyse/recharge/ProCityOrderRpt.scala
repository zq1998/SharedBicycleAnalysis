package offline.analyse.recharge

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  *根据省市日期计算订单指标
  * 1 订单总数
  * 2 订单的客户端类型
  * 3 订单的设备类型
  * 4 订单的网络类型
  * 5 订单的支付类型
  * 6 订单押金的总次数、总金额
  * 7 订单充值的总次数、总金额
  */
object ProCityOrderRpt {
  def main(args: Array[String]): Unit = {
    if(args.length!=1){
      println(
        """
          |参数：
          | 输入路径 | hdfs://hadoop1:9000/bike/washed/Order
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
    val result6=session.sql("select date,province,city,count(*) cashNum,SUM(amount) amount  " +
      "from log " +
      "where  rechargetype=1 " +
      "group by date,province,city ")
    val result7=session.sql("select date,province,city,COUNT(*) rechargeNum,SUM(amount) amount " +
      "from log " +
      "where  rechargetype=2 " +
      "group by date,province,city ")


    //    c存报表数据
    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    result1.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "SumPeoOrder", props)
    result2.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "clientPeoOrder", props)
    result3.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "devicePeoOrder", props)
    result4.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "networkPeoOrder", props)
    result5.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "rechargeTypePeoOrder", props)
    result6.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "cashPeoOrder", props)
    result7.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "rechargePeoOrder", props)


    session.close()
  }
}
