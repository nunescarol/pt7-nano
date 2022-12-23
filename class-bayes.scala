import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}

// Load pt7 data
val pt7_sparse = {
        spark.read
        .format("parquet")
        .load("hdfs://master:8020/bigdata/pt7-hash.parquet")
        .withColumnRenamed("_1", "label")
        .withColumnRenamed("_2", "url")
        .withColumnRenamed("_3", "words")
  }

val indexer = new StringIndexer().setInputCol("label").setOutputCol("labelIndex")

val indexed = indexer.fit(pt7_sparse).transform(pt7_sparse)

val training_set = indexed.drop("label").withColumnRenamed("labelIndex", "label")

val naive = new NaiveBayes()

// Fit the model
val naiveModel = naive.fit(training_set)

// Print the coefficients and intercept for logistic regression
println(s"Coefficients: ${naiveModel.coefficientMatrix} Intercept: ${naives.interceptVector}")
System.exit(0)
