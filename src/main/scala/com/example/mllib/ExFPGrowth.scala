package com.example.mllib

import com.example.Utils
import org.apache.spark.mllib.fpm.FPGrowth

/**
 * Created by spark on 31/10/15.
 */
object ExFPGrowth {

    def main(args: Array[String]) {

        val baskets = Utils.sc.textFile("/opt/spark/data/com.example.mllib/sample_fpgrowth.txt").map { line =>
            line.split(" ")
        }

        val fpGrowth = new FPGrowth().setMinSupport(.5)

        val model = fpGrowth.run(baskets)

        val rules = model.generateAssociationRules(.8)

        println("\nReglas obtenidas:")
         rules.foreach { rule =>
            println(rule.antecedent.toList + " -> " + rule.consequent.toList)
         }
    }

}
