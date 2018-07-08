// ref: https://github.com/P7h/Spark-MLlib-Twitter-Sentiment-Analysis/blob/master/src/main/scala/org/p7h/spark/sentiment/utils/PropertiesLoader.scala

package iic3423.project.utils

import com.typesafe.config.{Config, ConfigFactory}

object PropertiesLoader {
    private val conf: Config = ConfigFactory.load("application.conf")

    val sentiment140TrainingFilePath = conf.getString("SENTIMENT140_TRAIN_DATA_ABSOLUTE_PATH")
    val sentiment140TestingFilePath = conf.getString("SENTIMENT140_TEST_DATA_ABSOLUTE_PATH")
    val nltkStopWords = conf.getString("NLTK_STOPWORDS_FILE_NAME ")

    val naiveBayesModelPath = conf.getString("NAIVEBAYES_MODEL_ABSOLUTE_PATH")
    val modelAccuracyPath = conf.getString("NAIVEBAYES_MODEL_ACCURACY_ABSOLUTE_PATH ")

    val tweetsRawPath = conf.getString("TWEETS_RAW_ABSOLUTE_PATH")
    val saveRawTweets = conf.getBoolean("SAVE_RAW_TWEETS")

    val tweetsClassifiedPath = conf.getString("TWEETS_CLASSIFIED_ABSOLUTE_PATH")

    val consumerKey = conf.getString("CONSUMER_KEY")
    val consumerSecret = conf.getString("CONSUMER_SECRET")
    val accessToken = conf.getString("ACCESS_TOKEN_KEY")
    val accessTokenSecret = conf.getString("ACCESS_TOKEN_SECRET")

    val microBatchTimeInSeconds = conf.getInt("STREAMING_MICRO_BATCH_TIME_IN_SECONDS")
    val totalRunTimeInMinutes = conf.getInt("TOTAL_RUN_TIME_IN_MINUTES")
}