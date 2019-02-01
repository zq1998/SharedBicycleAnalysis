package offline.analyse.recharge

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 1 产生支付的日用户数
  * 2 各时段产生押金支付的数量、总金额、付款方式
  * 3 各时段产生充值支付的数量、总金额、付款方式
  */
object PeopleRechargeRpt {
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
    val result1=session.sql("select date,province,city,COUNT(distinct userid) usersum " +
      "from log " +
      "group by date,province,city ")
    //    FROM_UNIXTIME接受bigint类型参数
    val result2=session.sql("select date,province,city,rechargesource,COUNT(*) sum ,SUM(amount) amount,FROM_UNIXTIME(timestamp ,'H' ) hour " +
      "from log " +
      "where  rechargetype=1 " +
      "group by date,province,city,FROM_UNIXTIME(timestamp ,'H' ),rechargesource ")
    val result3=session.sql("select date,province,city,rechargesource,COUNT(*) sum ,SUM(amount) amount,FROM_UNIXTIME(timestamp ,'H' ) hour " +
      "from log " +
      "where  rechargetype=2 " +
      "group by date,province,city,FROM_UNIXTIME(timestamp ,'H' ),rechargesource ")

    //    c存报表数据
    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    result1.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "dayPeoRecharge", props)
    result2.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "hourcashPeoRecharge", props)
    result3.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "hourrechargePeoRecharge", props)
    session.close()
  }
}
