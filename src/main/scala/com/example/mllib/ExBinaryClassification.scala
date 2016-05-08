package com.example.mllib

import com.example.Utils
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
 * Created by spark on 30/10/15.
 */
object ExBinaryClassification {

    def main(args: Array[String]) {

        val datasets: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(Utils.sc, "/opt/spark/data/com.example.mllib/sample_binary_classification_data.txt")

        val Array(training, testing) = datasets.randomSplit(Array(0.8, 0.2))

        val lr = new LogisticRegressionWithSGD()
        lr.optimizer
                .setNumIterations(5)
                .setRegParam(0.05)
                .setUpdater(new SquaredL2Updater)

        val model: LogisticRegressionModel = lr.run(training)

        val predictionsAndLabels: RDD[(Double, Double)] = testing.map { labeledPoint =>
            ( model.predict(labeledPoint.features), labeledPoint.label)
        }

        val metrics: MulticlassMetrics = new MulticlassMetrics(predictionsAndLabels)

        println("Confusion matrix: \n" + metrics.confusionMatrix)

        println("\nComprobando valores:")
        predictionsAndLabels.foreach { case (prediction, label) =>
            println(prediction + " : " + label)
        }
    }

}
