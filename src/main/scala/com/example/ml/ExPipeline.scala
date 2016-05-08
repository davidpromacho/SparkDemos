package com.example.ml

import com.example.Utils
import org.apache.spark.ml.{Estimator, PipelineModel, Pipeline}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{Tokenizer, HashingTF}
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Created by spark on 31/10/15.
 */
object ExPipeline {

    def main(args: Array[String]) {
        // Prepare training documents from a list of (id, text, label) tuples.
        val training = Utils.sqlContext.createDataFrame(Seq(
            (0L, "a b c d e spark", 1.0),
            (1L, "b d", 0.0),
            (2L, "spark f g h", 1.0),
            (3L, "hadoop mapreduce", 0.0)
        )).toDF("id", "text", "label")

        // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
        val tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words")
        val hashingTF = new HashingTF()
                .setNumFeatures(100)
                .setInputCol(tokenizer.getOutputCol)
                .setOutputCol("features")
        val lr = new LogisticRegression()
                .setMaxIter(20)
                .setRegParam(0.01)
        val pipeline = new Pipeline()
                .setStages(Array(tokenizer, hashingTF, lr))

        // Fit the pipeline to training documents.
        val model: PipelineModel = pipeline.fit(training)
        val algo: Estimator[PipelineModel] = model.parent

        // Prepare test documents, which are unlabeled (id, text) tuples.
        val test = Utils.sqlContext.createDataFrame(Seq(
            (4L, "spark i j k"),
            (5L, "l m n"),
            (6L, "mapreduce spark"),
            (7L, "apache hadoop")
        )).toDF("id", "text")

        // Make predictions on test documents.
        println("Schema del testing set:")
        test.printSchema()

        val predictions: DataFrame = model.transform(test)
        println("Schema de las predicciones:")
        predictions.printSchema()

        predictions.select("id", "text", "probability", "prediction").show()

    }

}
