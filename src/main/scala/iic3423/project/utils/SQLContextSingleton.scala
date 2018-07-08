// ref: https://github.com/P7h/Spark-MLlib-Twitter-Sentiment-Analysis/blob/master/src/main/scala/org/p7h/spark/sentiment/utils/SQLContextSingleton.scala

package iic3423.project.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object SQLContextSingleton {
    @transient
    @volatile private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
        if (instance == null) {
            synchronized {
                if (instance == null) {
                    instance = SQLContext.getOrCreate(sparkContext)
                }
            }
        }
        instance
    }
}