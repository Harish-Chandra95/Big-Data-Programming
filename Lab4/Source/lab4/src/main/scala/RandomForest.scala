import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.{Row, SparkSession}

object RandomForest {


  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Winutils");


    val spark = SparkSession
      .builder()
      .appName("Lab4")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._


    val data1 = spark.read.format("csv").
      option("header", "true").

      load("E:\\abb.csv")

    val data = data1.select(data1("Reason_for_absence"), data1("Social_drinker"), data1("Social_smoker"), data1("Body_mass_index"), data1("Absenteeism_time_in_hours"))

    println("1")

    val toVec4 = udf[Vector, Int, Int, Int, Int] { (a, b, c, d) =>
      Vectors.dense(a, b, c, d)
    }

    val encodeLabel = udf[Double, Int] { a => a.toDouble }

    val df = data.withColumn(
      "features",
      toVec4(
        data("Reason_for_absence"),
        data("Social_drinker"),
        data("Social_smoker"),
        data("Body_mass_index")
      )
    ).withColumn("label", encodeLabel(data("Absenteeism_time_in_hours"))).select("features", "label")

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(df)


    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(df)

    println("2")

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test set accuracy = $accuracy")
    println(s"Test Error = ${(1.0 - accuracy)}")

    val predictionAndLabels = predictions.select("prediction", "label").map { case Row(l: Double, p: Double) => (l, p) }.rdd
    val metrics = new MulticlassMetrics(predictionAndLabels)
    println(metrics.confusionMatrix)
    println(s"The precision Score for random forest= ${metrics.weightedPrecision}")
    println(s"The Recall score for Random  forest is= ${metrics.weightedRecall}")


    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println(s"Learned classification forest model:\n ${rfModel.toDebugString}")


    println(s"Test set accuracy = $accuracy")
    println(s"Test Error = ${(1.0 - accuracy)}")
    println(metrics.confusionMatrix)
    println(s"The precision Score for random forest= ${metrics.weightedPrecision}")
    println(s"The Recall score for Random  forest is= ${metrics.weightedRecall}")
  }
}
