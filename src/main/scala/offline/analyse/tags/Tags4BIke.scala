package offline.analyse.tags

import org.apache.spark.sql.Row

/**
  * 骑行标签
  */
object Tags4BIke extends Tags{
  override def makeTags(args: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
    val row = args(0).asInstanceOf[Row]
    val unlockId = row.getAs[Int]("unlock")
    val distance = row.getAs[Double]("distance")
    unlockId match {
      case 1 => map += "LS"+"扫码" ->1
      case 2 => map += "LS"+"蓝牙" ->1
    }
    if(distance <=1000){
      map += "LA"+"短程" ->1
    }else if (distance < 2000){
      map += "LA"+"中程" ->1
    }else{
      map += "LA"+"长程" ->1
    }
    map
  }
}
