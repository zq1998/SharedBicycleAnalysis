package offline.analyse.tags

import com.typesafe.config.ConfigFactory
import offline.utils.JedisUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 生成标签存入hbase
  */
object TagsCtx {
  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println(
        """
          |参数：
          | 输入路径  |  hdfs://hadoop1:9000/bike/washed/lock | hdfs://hadoop1:9000/bike/washed/Unlock
          | 日期
        """.stripMargin)
      sys.exit()
    }
    val Array(logInputPath,day)=args

    //    设置spark上下文的名称、运行模式、序列化方式
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[*]")
      .set("spark.sql.session.timeZone", "UTC")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //    创建spark上下文
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

//    判断hbase表是否存在，不存在则创建
    val load = ConfigFactory.load()
    val configuration = spark.sparkContext.hadoopConfiguration
    val hbTableName=load.getString("hbase.table.name")

    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.zookeeper.host"))
    val hbConn = ConnectionFactory.createConnection(configuration)
    val hbAdmin = hbConn.getAdmin

    if(hbAdmin.tableExists(TableName.valueOf(hbTableName))){
      println(s"$hbTableName 不存在。。。")
      println(s"$hbTableName 正在创建。。。")
      val tableDescriptor=new HTableDescriptor(TableName.valueOf(hbTableName))
      val columnDescriptor = new HColumnDescriptor("cf")
      tableDescriptor.addFamily(columnDescriptor)

      //释放连接
      hbAdmin.close()
      hbConn.close()
    }

//    指定key输入类型
    val jobConf = new JobConf(configuration)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
//    指定表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbTableName)


//    生成标签
    import spark.implicits._
    val value = spark.read.parquet(logInputPath).where(
      """
          userid !=""
      """.stripMargin
    ).rdd.mapPartitions(par => {
      val jedis = JedisUtils.getJedisClient
      val result = par.map(row => {
        val userid = row.getAs[String]("userid")

        val clients = Tags4Client.makeTags(row)
        val bikes = Tags4BIke.makeTags(row)
        val business = Tags4Business.makeTags(row, jedis)
        (userid, (clients ++ bikes ++ business).toList)
      })
      jedis.close()
      result
    }).reduceByKey((a, b) => {
      //      (a._2 ++ b._2).groupBy(_._1).map{
      //        case (k,sameTags) =>(k,sameTags.map(_._2).sum)
      //      }.toList
      (a ++ b).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList
    }).map{
      case (userid,userTags) =>{
        val put=new Put(Bytes.toBytes(userid))
        val tags=userTags.map(t=> t._1 + ":" + t._2).mkString(",")
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes(s"day $day"),Bytes.toBytes(tags))
        //row key对象
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(jobConf)


    spark.close()
  }
}
