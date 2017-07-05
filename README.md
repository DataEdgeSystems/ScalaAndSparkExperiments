# Scala and Spark (2.1) Experiments 

Code snippets of Scala and Spark code (on community editon of Databricks). 

## ReturnCalculation.scala 

**Problem**: Calculate stock returns from stock prices in Apache SPARK for each company in the data set.  

Data File: data/02_firmData.csv

```
+-------+----------+------+--------------------+
|   firm|      date| price|              return|
+-------+----------+------+--------------------+
|Daimler|2014-05-14| 61.55|                null|
|Daimler|2014-05-15|60.868|-0.01114226757259431|
|Daimler|2014-05-16|60.139|-0.01204903550403...|
|Daimler|2014-05-19|60.886|0.012344713676928779|
|Daimler|2014-05-20|61.292|0.006646065485169854|
|Daimler|2014-05-21|61.928| 0.01032309118648241|
|Daimler|2014-05-22|62.269|0.005491289755520844|
|Daimler|2014-05-23|62.841|0.009144017033805655|
|Daimler|2014-05-26|64.197|0.021348753940381736|
|Daimler|2014-05-27|64.151|-7.16801227437729E-4|
|Daimler|2014-05-28|63.902|-0.00388901960645...|
|Daimler|2014-05-29|64.234| 0.00518200572731672|
|Daimler|2014-05-30| 64.28|7.158753437680434E-4|
|Daimler|2014-06-02|63.929|-0.00547544832157...|
|Daimler|2014-06-03|64.261|0.005179822799156585|
|Daimler|2014-06-04| 63.92|-0.00532061388887...|
|Daimler|2014-06-05|64.842|0.014321240091064268|
|Daimler|2014-06-06|64.778|-9.87502008962297...|
|Daimler|2014-06-09| 64.64|-0.00213262532728...|
|Daimler|2014-06-10| 64.63|-1.54714938035893...|
+-------+----------+------+--------------------+
```

## UnivariateSampling.scala

Scala version of univariate sampling from https://www.mapr.com/ebooks/spark/08-unsupervised-anomaly-detection-apache-spark.html

## Frequency.scala

**Problem:** Calculate univariate frequency table for categorical data in Apache SPARK and test the performance.

## AVFScoring.scala 

**Problem:** Perform outlier detection on categorical data in Apache SPARK.

Outlier detection for categorical data (attribute value frequency). [Databricks Dashboard](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/8100472355965647/225569915347134/8510365300034746/latest.html)


