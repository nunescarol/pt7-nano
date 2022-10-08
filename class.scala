import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}

// Load training data
val pt7_sparse = {
	spark.read
	.format("parquet")
	.load("hdfs://master:8020/bigdata/pt7-hash.parquet")
	.withColumnRenamed("_1", "label")
	.withColumnRenamed("_2", "url")
	.withColumnRenamed("_3", "words")

val indexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("labelIndex")
  .fit(pt7_sparse)

val indexed = indexer.transform(pt7_sparse)

indexed.show()

val lr = new LogisticRegression()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

// Fit the model
val lrModel = lr.fit(training)

// Print the coefficients and intercept for logistic regression
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

// We can also use the multinomial family for binary classification
val mlr = new LogisticRegression()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)
  .setFamily("multinomial")

val mlrModel = mlr.fit(training)

// Print the coefficients and intercepts for logistic regression with multinomial family
println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
println(s"Multinomial intercepts: ${mlrModel.interceptVector}")