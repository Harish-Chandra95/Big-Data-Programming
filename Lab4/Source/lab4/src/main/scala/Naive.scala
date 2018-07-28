import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.udf

object Naive {


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

    // Train a NaiveBayes model
    val model = new NaiveBayes()
      .fit(trainingData)

    // Select example rows to display.
    val predictions = model.transform(testData)
    predictions.show(10)

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test set accuracy = $accuracy")
    println(s"Test Error = ${(1.0 - accuracy)}")

    val predictionAndLabels = predictions.select("prediction", "label").map { case Row(l: Double, p: Double) => (l, p) }.rdd
    val metrics = new MulticlassMetrics(predictionAndLabels)

    println(s"Test set accuracy = $accuracy")
    println(s"Test Error = ${(1.0 - accuracy)}")
    println(s"confusion matrix=${(metrics.confusionMatrix)}")
    println(s"The precision Score for Naive Bayes= ${metrics.weightedPrecision}")
    println(s"The Recall score for Naive Bayes is= ${metrics.weightedRecall}")




    println(s"Test set accuracy = $accuracy")
    println(s"Test Error = ${(1.0 - accuracy)}")
    println(s"confusion matrix=${(metrics.confusionMatrix)}")
    println(s"The precision Score for Naive Bayes= ${metrics.weightedPrecision}")
    println(s"The Recall score for Naive Bayes is= ${metrics.weightedRecall}")



  }
}

