    /*
    Univariate Sampling of a DataFrame
    Sample with replacement in each column of a SPARK DataFrame
     */
    def univariateSampling(df: DataFrame, fraction: Double): DataFrame = {
      val colNames = df.columns
      val nCols: Int = colNames.length
      val n = (df.count * fraction).toInt
      // For implicit conversions from RDDs to DataFrames
      import spark.implicits._
      def f(result: DataFrame, col: Int): DataFrame = col match {
        case col if (col < nCols) => {
          val column = df.sample(true, 1.2 * fraction)
            .limit(n)
            .select(colNames(0))
            .rdd
            .zipWithIndex
            .map { case (x, y) => (y, x(0).toString) }
            .toDF("id", "value")
          if (result == null) {
            f(column, col + 1)
          } else {
            f(result.join(column, "id"), col + 1)
          }
        }
        case _ => result
      }

      f(null, 0).drop("id")
    }
