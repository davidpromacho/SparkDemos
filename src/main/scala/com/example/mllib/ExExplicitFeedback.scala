package com.example.mllib

import com.example.Utils
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
 * Created by spark on 31/10/15.
 */
object ExExplicitFeedback {

    def main(args: Array[String]) {

        val ratings: RDD[Rating] = Utils.sc.textFile("/opt/spark/data/com.example.mllib/als/test.data")
            .map { line: String =>
                val parts: Array[String] = line.split(",")
                new Rating(parts(0).toInt, parts(1).toInt, parts(2).toDouble / 5.0d)
            }

        val als: ALS = new ALS()
                .setImplicitPrefs(true)
                .setIterations(10)
                .setRank(3)
                .setLambda(0.1)
                .setAlpha(0.05)

        val model: MatrixFactorizationModel = als.run(ratings)

        // testing set = (user, product)
        val testing: RDD[(Int, Int)] = ratings.map { r: Rating =>
            (r.user, r.product)
        }

        // Get 10 recommendations for all users = (user, products)
        val recommendations: RDD[(Int, Array[Rating])] = model.recommendProductsForUsers(10)

        println("\nRecomendaciones obtenidas: ")
        recommendations.foreach { reco =>
            println(s"user ${reco._1}: ")
            reco._2.foreach { r =>
                println(s"\t${r.product}:${r.rating}")
            }
        }

        // Metrics on relevant products
        // val metrics: RankingMetrics[Int] = ???
    }

}
