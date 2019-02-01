package offline.analyse.repair

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  *根据省市日期计算支付指标
  * 1 报修数量分布
  * 2 报修时段分布
  * 3 报修类型
  */
object ProCityRepairRpt {
  def main(args: Array[String]): Unit = {
    if(args.length!=1){
      println(
        """
          |参数：
          | 输入路径 | hdfs://hadoop1:9000/bike/washed/Repair
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
    val result1=session.sql("select date,province,city,COUNT(*) sum " +
      "from log " +
      "group by date,province,city ")
    //    FROM_UNIXTIME接受bigint类型参数
    val result2=session.sql("select date,province,city,COUNT(*) sum ,FROM_UNIXTIME(timestamp ,'H' ) hour " +
      "from log " +
      "group by date,province,city,FROM_UNIXTIME(timestamp ,'H' ) ")
    val result3=session.sql("select date,province,city,repairtype,COUNT(*) sum ,FROM_UNIXTIME(timestamp ,'H' ) hour " +
      "from log " +
      "group by date,province,city,FROM_UNIXTIME(timestamp ,'H' ),repairtype ")


    //    c存报表数据
    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    result1.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "AreaRepair", props)
    result2.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "HourRepair", props)
    result3.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), "TypeRepair", props)

    session.close()
  }
}
