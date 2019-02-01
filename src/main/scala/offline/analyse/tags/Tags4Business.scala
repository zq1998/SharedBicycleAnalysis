package offline.analyse.tags

import ch.hsr.geohash.GeoHash
import offline.utils.BaiduGeoApi
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis



/**
  * 商圈标签
  */
object Tags4Business extends Tags {
  override def makeTags(args: Any*): Map[String, Int] = {
    var map=Map[String,Int]()

    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]
    val lat = row.getAs[Double]("latitude")
    val long= row.getAs[Double]("longitude")

    if(lat > 3 && lat < 54 && long > 73 && long < 136){
      val geoHashCode = GeoHash.withCharacterPrecision(lat,long,8).toBase32
      val business = jedis.get(geoHashCode)

      if(StringUtils.isNotEmpty(business)){
        business.split(",").foreach(bs => map += "BS"+bs ->1)
      }else{
        val business = BaiduGeoApi.getBusiness(lat+","+long)
        jedis.set(geoHashCode,business)
        business.split(",").foreach(bs => map += "BS"+bs ->1)
      }

    }
    map
  }
}
