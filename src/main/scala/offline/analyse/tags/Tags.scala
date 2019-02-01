package offline.analyse.tags

/**
  * 打标签方法定义
  */
trait Tags {
  def makeTags(args: Any*):Map[String,Int]

}
