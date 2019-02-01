package offline.pretreatment.SchemaUtils

import org.apache.spark.sql.types._

/**
  * parquet文件的schema信息
  */
object RepairSchema {
  val logStructType=StructType(Seq(
    StructField("sessionid", StringType),
    StructField("userid", StringType),
    StructField("bikeid", StringType),
    StructField("timestamp", LongType),
    StructField("date", StringType),
    StructField("province", StringType),
    StructField("city", StringType),
    StructField("region", StringType),
    StructField("latitude", DoubleType),
    StructField("longitude", DoubleType),
    StructField("ip", StringType),
    StructField("client",IntegerType),
    StructField("mac", StringType),
    StructField("deviceType", IntegerType),
    StructField("networkingmannerid", IntegerType),
    StructField("version", StringType),
    StructField("repairtype", IntegerType)
  ))
}
