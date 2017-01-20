import org.apache.spark.sql.DataFrame  
def frequency(data: DataFrame): Map[String, Map[Int, Int]] = {
    def f(ct: Map[String, Map[Int, Int]], cols: Array[String]): Map[String, Map[Int, Int]] = cols match {
      case cols if cols.length == 0 => ct
      case _ =>
        val col = cols.head
        f(ct + (col -> data.groupBy(col).sum(col).collect.map(x => x.getInt(0) -> x.getAs[Int](1)).toMap), cols.tail)
    }
    val ct: Map[String, Map[Int, Int]] = Map()
    f(ct, data.columns)
  }
  
  
  // this function is much faster than frequency as reduceByKey perform first a combine on each partition before shuffle
  // see: https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
  def frequencyRDD(data: DataFrame): Map[String, Map[Int, Int]] = {
    def f(ct: Map[String, Map[Int, Int]], cols: Array[String]): Map[String, Map[Int, Int]] = cols match {
      case cols if cols.length == 0 => ct
      case _ =>
        val col = cols.head
        f(ct + (col -> data.select(col).rdd.map(x => (x.getInt(0), 1)).reduceByKey(_ + _).collect.map(x => x._1 -> x._2).toMap), cols.tail)
    }
    val ct: Map[String, Map[Int, Int]] = Map()
    f(ct, data.columns)
  }
  
// example data
import org.apache.spark.sql.functions._
val randInt: Double => Int = x => (x * 10).toInt
val randIntUDF = udf(randInt)

val n:Long = 100000
val dfId = sqlContext.range(0, n)
val df = dfId.select($"id", randIntUDF(rand()) as "n1", randIntUDF(rand()) as "n2", randIntUDF(rand()) as "n3", randIntUDF(rand()) as "n4").drop($"id").cache()

val ft = frequency(df) // Command took 1.79 seconds on Databricks Community Edition
val ftRdd = frequencyRDD(df) // Command took 0.46 seconds on Databricks Community Edition
