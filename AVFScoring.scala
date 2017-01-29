import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{udf, lit, col}

object AVFOutlier {
  def computeScore(df: DataFrame): DataFrame = {
    // calculate frequency table for each columns saved as a map of map
    val ft = frequencyMap(df)

    // calculate score
    val ftMap = ftCreateLookup(ft)
    val dfCols = df.columns
    df.withColumn("score", dfCols.map(x => ftMap(col(x), lit(x))).reduce((c1, c2) => c1 + c2))
  }

  // https://github.com/spirom/LearningSpark/blob/master/src/main/scala/dataframe/UDF.scala
  def ftCreateLookup(m: Map[String, Map[Int, Int]])() = {
    val f: (Int, String) => Int = {(v, c) => m(c).getOrElse(v, "0").toString.toInt}
    udf(f)
  }

def frequencyMap(data: DataFrame): Map[String, Map[Any, Int]] = {
    def f(ct: Map[String, Map[Any, Int]], cols: Array[String]): Map[String, Map[Any, Int]] = cols match {
      case cols if cols.length == 0 => ct
      case _ =>
        val col = cols.head
        f(ct + (col -> data.groupBy(col).count().collect.map(x => x.get(0) -> x.getAs[Int](1)).toMap), cols.tail)
    }
    val ct: Map[String, Map[Any, Int]] = Map()
    f(ct, data.columns)
  }
}
