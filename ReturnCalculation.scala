import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

val firm = StructField("firm", DataTypes.StringType)
val date = StructField("date", DataTypes.DateType)
val price = StructField("price", DataTypes.DoubleType)
val dataFields = Array(firm, date, price)
val dataFileSchema = StructType(dataFields)

// Read Data File with Specified Date Format
val firmData = spark.read
  .option("dateFormat", "dd.MM.yyyy")
  .option("delimiter", ";")
  .option("header", false)
  .schema(dataFileSchema)
  .csv("./02_firmData.csv")

// Define Window Function for Calculating log return by Firm
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val returnWindow = Window.partitionBy("firm").orderBy(asc("date"))
val logReturn = log('price / lead('price, -1).over(returnWindow))

// Calculate log return
val firmReturn = firmData.withColumn("return", logReturn)
