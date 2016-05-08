package com.example.mllib

import com.example.Utils
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * Created by spark on 31/10/15.
 */
object ExClustering {

    def main(args: Array[String]) {

        val dataset: RDD[Vector] = Utils.sc.textFile("/opt/spark/data/com.example.mllib/kmeans_data.txt")
            .map { line =>
                Vectors.dense(line.split(" ").map(_.toDouble))
            }

        val kmeans = new KMeans()
            .setK(2)
            .setMaxIterations(10)

        val clusters = kmeans.run(dataset)

        val wssse = clusters.computeCost(dataset)

        println(s"\nWSSSE: $wssse")

        println("\nResultado: ")
        dataset.map { (point: Vector) =>
            new LabeledPoint(clusters.predict(point), point)
        }.foreach { labeledPoint =>
            println(labeledPoint.toString())
        }

    }

}
