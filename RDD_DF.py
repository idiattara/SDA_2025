from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

# Données initiales
data = [("Alice", 25), ("Bob", 30), ("Catherine", 29), ("David", 22)]

# ------------------- RDD -------------------
rdd = spark.sparkContext.parallelize(data)

# Transformation : filtre et ajout de 1 à l'âge
rdd_transformed = rdd.filter(lambda x: x[1] > 27).map(lambda x: (x[0], x[1] + 1))

print("=== RDD Transformé ===")
display(rdd_transformed.toDF(["nom", "age_plus_un"]))

# ------------------- DataFrame -------------------
df = rdd.toDF(["nom", "age"])

df_transformed = df.filter(col("age") > 27).withColumn("age_plus_un", col("age") + 1)

print("=== DataFrame Transformé ===")
display(df_transformed)
