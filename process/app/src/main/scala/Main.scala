import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StopWordsRemover, HashingTF, IDF}
import org.apache.spark.ml.Pipeline
import com.johnsnowlabs.nlp.annotators.sda.pragmatic.SentimentDetector
import com.johnsnowlabs.nlp.embeddings.BertEmbeddings
import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.annotators.{Tokenizer => JohnSnowTokenizer}
import org.apache.spark.ml.feature.Tokenizer;

import com.johnsnowlabs.nlp.annotator.SentenceDetector

object Main {
  def main(args: Array[String]) {

    // Base schema
    // https://hackernews.api-docs.io/v0/items/base
    val baseSchema = new StructType()
      .add("id", IntegerType)
      .add("deleted", BooleanType)
      .add("type", StringType)
      .add("by", StringType)
      .add("time", IntegerType)
      .add("dead", BooleanType)
      .add("kids", ArrayType(IntegerType))
      .add("text", StringType) // Job, Comment, Poll
      .add("url", StringType) // Job, Story,
      .add("title", StringType) // Job, Story, Poll
      .add("descendants", IntegerType) // Story, Poll
      .add("score", IntegerType) // Story, Poll, PollOption
      .add("parent", IntegerType) // Comment, PollOption
      .add("parts", ArrayType(IntegerType)) // Poll

    val spark = SparkSession.builder
      .appName("Simple Application")
      .config("spark.master", "local")
      .config("spark.driver.memory", "15g")
      .config("spark.executor.memory", "6g")
      .getOrCreate()

    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    import spark.implicits._

    // Setup kafka
    val topics = Array("hn_topic")
    val rawDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "hn_topic")
      .option("startingOffsets", "latest") // From starting
      .load()

    // // Convert bytes to string and parse the json
    val processedDf = rawDf
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .select(from_json($"value", baseSchema) as "data")
      .select("data.*")

    // processedDf.printSchema()

    // Fill na values
    val fillnaDf = processedDf.na
      .fill("", Seq("text", "title", "url", "by", "type"))
      .na
      .fill(0, Seq("score"))
      .withColumn("time", $"time".cast(TimestampType))
      .select("text")

    // val fillnaDf = Seq(
    //   "Spark NLP is an open-source text processing library for advanced natural language processing."
    // ).toDF("text")

    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val sentence = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("tokens")

    val johnSnowTokenizer = new JohnSnowTokenizer()
      .setInputCols("sentence")
      .setOutputCol("token")
    // Setup stope words remover
    // val remover = new StopWordsRemover()
    //   .setInputCol("token")
    //   .setOutputCol("filtered_token")

    // Get term frequency
    val hashingTF = new HashingTF()
      .setInputCol("tokens")
      .setOutputCol("raw_tf")

    val idf = new IDF()
      .setInputCol("raw_tf")
      .setOutputCol("tf_idf")

    val embeddings = BertEmbeddings
      .pretrained("small_bert_L2_128", "en")
      .setInputCols("token", "document")
      .setOutputCol("bert_embeddings")

    val pipeline = new Pipeline()
      .setStages(
        Array(
          documentAssembler,
          sentence,
          tokenizer,
          johnSnowTokenizer,
          // remover,
          hashingTF,
          embeddings
        )
      )
      .fit(fillnaDf)

    val result = pipeline.transform(fillnaDf)

    // result.show()

    // cleanDf.printSchema()

    // Calculate TF IDF

    // val idfModel = idf.fit(featurizedData)

    // // Compute
    // // Word count
    // val wordCount = result
    //   .withColumn("wordCount", size(col("token")))
    val query = result.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "true")
      .start()
      .awaitTermination()

    spark.stop

  }
}
