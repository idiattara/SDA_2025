%scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.sqlContext.implicits
//Données initiales
val data = Seq(("Alice", 25), ("Bob", 30), ("Catherine", 29), ("David", 22))
// ------------------- RDD -------------------
val rdd = spark.sparkContext.parallelize(data)
// ------------------- DataFrame -------------------
val df = rdd.toDF("nom", "age")

val df_transformed = df.filter(col("age") > 27).withColumn("age_plus_un", col("age") + 1)

print("=== DataFrame Transformé ===")
display(df_transformed)

