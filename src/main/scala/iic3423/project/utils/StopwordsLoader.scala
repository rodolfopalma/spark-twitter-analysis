// ref: https://github.com/P7h/Spark-MLlib-Twitter-Sentiment-Analysis/blob/master/src/main/scala/org/p7h/spark/sentiment/utils/PropertiesLoader.scala

package iic3423.project.utils

import scala.io.Source

object StopwordsLoader {
    def loadStopWords(stopWordsFileName: String): List[String] = {
        Source.fromInputStream(getClass.getResourceAsStream("/" + stopWordsFileName)).getLines().toList
    }
}