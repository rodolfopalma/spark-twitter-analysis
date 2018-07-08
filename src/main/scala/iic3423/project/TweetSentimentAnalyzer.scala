// ref: https://github.com/P7h/Spark-MLlib-Twitter-Sentiment-Analysis/blob/master/src/main/scala/org/p7h/spark/sentiment/TweetSentimentAnalyzer.scala
package iic3423.project

import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.serializer.KryoSerializer
import iic3423.project.utils._
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.flume._

// spark-submit --class "iic3423.project.TweetSentimentAnalyzer" --master local[2] target/scala-2.11/spark-twitter-analysis_2.11-1.0.jar quickstart.cloudera 8081 hdfs://quickstart.cloudera:8020/iic3423/predicted
object TweetSentimentAnalyzer {
    def main(args: Array[String]) {
        if (args.length < 3) {
            System.err.println("Usage: TweetSentimentAnalyzer <hostname> <port> <target_dir>")
            System.exit(1)
        }

        val ssc = StreamingContext.getActiveOrCreate(createSparkStreamingContext)

        // Load Naive Bayes Model from the location specified in the config file.
        val naiveBayesModel = NaiveBayesModel.load(ssc.sparkContext, PropertiesLoader.naiveBayesModelPath)
        val stopWordsList = ssc.sparkContext.broadcast(StopwordsLoader.loadStopWords(PropertiesLoader.nltkStopWords))

        val stream = FlumeUtils.createStream(ssc, args(0), args(1).toInt)
        // ref: https://github.com/looker/spark_log_data/blob/master/src/main/scala/logDataWebinar.scala
        stream.count().map(cnt => "Received " + cnt + " events from Flume").print()
        val mapStream = stream.map(event => new String(event.event.getBody().array(), "UTF-8"))
        mapStream.foreachRDD{
            rdd => rdd.map{
                line =>
                    val tweetSplitted = line.split(",")
                    val tweetPolarity = tweetSplitted(0)
                    val tweetText = replaceNewLines(tweetSplitted(5))
                    val (predictedSentiment, features) = MLlibSentimentAnalyzer.computeSentiment(tweetText, stopWordsList, naiveBayesModel)
                    (tweetPolarity, predictedSentiment, features, tweetText)
            }.saveAsTextFile(args(2))
        }
        ssc.start()
        ssc.awaitTermination()
    }

    def createSparkStreamingContext(): StreamingContext = {
        val sparkConf = new SparkConf()
            .setAppName("TweetSentimentAnalyzer")
            .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
        val ssc = new StreamingContext(sparkConf, Durations.seconds(PropertiesLoader.microBatchTimeInSeconds))
        ssc
    }

    def replaceNewLines(tweetText: String): String = {
        tweetText.replaceAll("\n", "")
    }
}