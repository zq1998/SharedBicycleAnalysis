package pretreatment.SchemaUtils

import org.apache.spark.sql.types.{StructField, _}

/**
  * parquet文件的schema信息
  */
object UnlockSchema {
  val logStructType=StructType(Seq(
    StructField("sessionid", StringType),
    StructField("userid", StringType),
    StructField("bikeid", StringType),
    StructField("btimestamp",LongType ),
    StructField("etimestamp", LongType),
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
    StructField("unlock", IntegerType),
    StructField("networkingmannerid", IntegerType),
    StructField("version", StringType),
    StructField("distance", DoubleType)
  ))
}
