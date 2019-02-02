package live.utils


import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 使用FastDateFormat保证线程安全
  */
object DateUtils {
//  通过时间戳计算小时数
  def caculateHour(time :Long):String={

    val fastDateFormat = FastDateFormat.getInstance("H")
    fastDateFormat.format(new Date(time))

//    val simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd  HH:mm:ss")
//    simpleDateFormat.format(new Date(time))
  }
  def caculateDate(time :Long):String= {
    val fastDateFormat = FastDateFormat.getInstance("yyyy-mm-dd")
    fastDateFormat.format(new Date(time))
  }
  def caculateTime(bTime :Long,eTime :Long):Double= {
    val bzTime=(eTime-bTime).toDouble
    bzTime/(1000*60*60)
  }

}
