package com.example.mllib

import com.example.Utils
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
 * Created by spark on 31/10/15.
 */
object ExMulticlassClassification {

    def main(args: Array[String]) {

        val datasets: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(Utils.sc, "/opt/spark/data/com.example.mllib/sample_multiclass_classification_data.txt")

        val Array(training, testing) = datasets.randomSplit(Array(0.8, 0.2))


        // Random Forest
        val trees: RandomForestModel = RandomForest.trainClassifier(training, 3, Map[Int,Int](), 5, "auto", "gini", 5, 7)

        val predictionsAndLabels: RDD[(Double, Double)] = testing.map { labeledPoint =>
            (trees.predict(labeledPoint.features), labeledPoint.label )
        }

        val metrics: MulticlassMetrics = new MulticlassMetrics(predictionsAndLabels)

        println("")
        metrics.labels.foreach { label =>
            println(s"$label: precision(${metrics.precision(label)}) recall(${metrics.recall(label)}})")
        }
    }

}
