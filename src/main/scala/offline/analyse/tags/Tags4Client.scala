package offline.analyse.tags

import org.apache.spark.sql.Row

/**
  * 客户端类型标签
  */
object Tags4Client extends Tags {

  override def makeTags(args: Any*): Map[String, Int] = {
      var map=Map[String,Int]()
      val row = args(0).asInstanceOf[Row]
      val clientId = row.getAs[Int]("client")
      val deviceTypeId = row.getAs[Int]("deviceType")
      val networkingmannerid = row.getAs[Int]("networkingmannerid")
      clientId match {
        case 1 => map += "LN"+"iphone" ->1
        case 2 => map += "LN"+"android" ->1
        case 3 => map += "LN"+"小程序" ->1
      }
      deviceTypeId match {
        case 1 => map += "LC"+"iphone" ->1
        case 2 => map += "LC"+"android" ->1
        case 3 => map += "LC"+"wp" ->1
      }
      networkingmannerid match {
        case 1 => map += "LF"+"wifi" ->1
        case 2 => map += "LF"+"4G" ->1
        case 3 => map += "LF"+"3G" ->1
      }
      map

  }
}
