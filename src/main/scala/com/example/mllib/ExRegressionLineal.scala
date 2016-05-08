package com.example.mllib

import com.example.Utils
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD

/**
 * Created by spark on 30/10/15.
 */
object ExRegressionLineal {

    def main(args: Array[String]) {

        val dataset: RDD[LabeledPoint] = Utils.sc
                .textFile("/opt/spark/data/com.example.mllib/ridge-data/lpsa.data")
                .map { line =>
                        val parts: Array[String] = line.split(",")
                        val label = parts(0).toDouble
                        val doubles = parts(1).split(" ").map(_.toDouble)
                        val pepe = Vectors.dense(doubles)
                        LabeledPoint(label, pepe)
                }


        val Array(training, testing) = dataset.randomSplit(Array(0.8, 0.2))

        val model: LinearRegressionModel = LinearRegressionWithSGD.train(training, 5)


        val predictionsAndLabels: RDD[(Double, Double)] = testing.map { labeledPoint =>
            (model.predict(labeledPoint.features), labeledPoint.label)
        }

        val metrics: RegressionMetrics = new RegressionMetrics( predictionsAndLabels )

        println("MAE: " + metrics.meanAbsoluteError)
    }

}
