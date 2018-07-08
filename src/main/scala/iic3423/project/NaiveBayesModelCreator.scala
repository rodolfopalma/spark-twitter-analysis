// ref: https://github.com/P7h/Spark-MLlib-Twitter-Sentiment-Analysis/blob/master/src/main/scala/org/p7h/spark/sentiment/mllib/SparkNaiveBayesModelCreator.scala

package iic3423.project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._

import iic3423.project.utils._


// spark-submit --class "iic3423.project.NaiveBayesModelCreator" --packages com.databricks:spark-csv_2.11:1.4.0 --master local[2] target/scala-2.11/spark-twitter-analysis_2.11-1.0.jar
object NaiveBayesModelCreator {
    def main(args: Array[String]) {
        val sc = createSparkContext()
        val stopWordsList = sc.broadcast(StopwordsLoader.loadStopWords(PropertiesLoader.nltkStopWords))
        createAndSaveNBModel(sc, stopWordsList)
        validateAccuracyOfNBModel(sc, stopWordsList)
    }

    def replaceNewLines(tweetText: String): String = {
        tweetText.replaceAll("\n", "")
    }

    def createSparkContext(): SparkContext = {
        val conf = new SparkConf()
            .setAppName("NaiveBayesModel")
            .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    def createAndSaveNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
        val tweetsDF: DataFrame = loadSentiment140File(sc, PropertiesLoader.sentiment140TrainingFilePath)
        val labeledRDD = tweetsDF.select("polarity", "status").rdd.map {
            case Row(polarity: Int, tweet: String) => 
                val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(tweet, stopWordsList.value)
                LabeledPoint(polarity, MLlibSentimentAnalyzer.transformFeatures(tweetInWords))
        }
        labeledRDD.cache()
        val naiveBayesModel: NaiveBayesModel = NaiveBayes.train(labeledRDD, lambda=1.0, modelType="multinomial")
        naiveBayesModel.save(sc, PropertiesLoader.naiveBayesModelPath)
    }

    def validateAccuracyOfNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
        val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(sc, PropertiesLoader.naiveBayesModelPath)
        val tweetsDF: DataFrame = loadSentiment140File(sc, PropertiesLoader.sentiment140TestingFilePath)
        val actualVsPredictionRDD = tweetsDF.select("polarity", "status").rdd.map {
            case Row(polarity: Int, tweet: String) =>
                val tweetText = replaceNewLines(tweet)
                val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(tweetText, stopWordsList.value)
                (polarity.toDouble,
                naiveBayesModel.predict(MLlibSentimentAnalyzer.transformFeatures(tweetInWords)),
                tweetText)
        }
        val accuracy = 100.0 * actualVsPredictionRDD.filter(x => x._1 == x._2).count() / tweetsDF.count()
        println(f"""\n\t<==******** Prediction accuracy compared to actual: $accuracy%.2f%% ********==>\n""")
        saveAccuracy(sc, actualVsPredictionRDD)
    }

    def loadSentiment140File(sc: SparkContext, sentiment140FilePath: String): DataFrame = {
        val sqlContext = SQLContextSingleton.getInstance(sc)
        val tweetsDF = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "false")
            .option("inferSchema", "true")
            .load(sentiment140FilePath)
            .toDF("polarity", "id", "date", "query", "user", "status")
        tweetsDF.drop("id").drop("date").drop("query").drop("user")
    }

    def saveAccuracy(sc: SparkContext, actualVsPredictionRDD: RDD[(Double, Double, String)]): Unit = {
        val sqlContext = SQLContextSingleton.getInstance(sc)
        import sqlContext.implicits._
        val actualVsPredictionDF = actualVsPredictionRDD.toDF("Actual", "Predicted", "Text")
        actualVsPredictionDF.coalesce(1).write
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("delimiter", "\t")
            .option("codec", classOf[GzipCodec].getCanonicalName)
            .mode(SaveMode.Append)
            .save(PropertiesLoader.modelAccuracyPath)
    }
}