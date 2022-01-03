from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, from_json
from pyspark.ml import Pipeline

from pyspark.sql.types import *
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp

import os

spark_version = '3.2.0'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:{},org.apache.kafka:kafka-clients:2.8.1,com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.4'.format(spark_version)

# run:
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.4 app.py
# /opt/spark/sbin/start-master.sh
# /opt/spark/sbin/start-worker.sh spark://ubu:7077



spark = SparkSession \
    .builder \
    .appName("HNProcess") \
    .master("spark://ubu:7077") \
    .config("spark.driver.memory","8g")\
    .config("spark.executor.memory","8g")\
    .config("spark.worker.cleanup.enabled", True)\
    .config("spark.driver.maxResultSize","20g")\
    .config("spark.kryoserializer.buffer.max", "2000M")\
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.4")\
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN') \

# Base schema
# https:#hackernews.api-docs.io/v0/items/base
schema = StructType() \
    .add("id", IntegerType()) \
    .add("deleted", BooleanType()) \
    .add("type", StringType()) \
    .add("by", StringType()) \
    .add("time", IntegerType()) \
    .add("dead", BooleanType()) \
    .add("kids", ArrayType((IntegerType()))) \
    .add("text", StringType())  \
    .add("url", StringType()) \
    .add("title", StringType()) \
    .add("descendants", IntegerType()) \
    .add("score", IntegerType()) \
    .add("parent", IntegerType()) \
    .add("parts", ArrayType((IntegerType())))

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")  \
    .option("subscribe", "hn_topic") \
    .option("startingOffsets", "latest") \
    .load()

dfA = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .select(from_json("value", schema).alias("posts")) \
        .select("posts.*")
dfB = dfA.na.fill(value=".", subset=["text", "title", "url", "by", "type"]) \
      .na \
      .fill(0, subset=["score"]) \
      .select("text")

tokenizer = Tokenizer() \
    .setInputCols("document") \
    .setOutputCol("token")

documentAssembler = DocumentAssembler()\
    .setInputCol("text") \
    .setOutputCol("document")

sentence = SentenceDetector() \
    .setInputCols("document") \
    .setOutputCol("sentence")

sentenceEmbeddings = BertSentenceEmbeddings.load("models/sent_small_bert_L2_768/") \
    .setInputCols("sentence") \
    .setOutputCol("bert_sentence_embeddings")

universalSentenceEmbeddings = UniversalSentenceEncoder.load("models/tfhub_use/") \
    .setInputCols("sentence") \
    .setOutputCol("sentence_embeddings")


sentiment = SentimentDLModel() \
    .load("models/sentimentdl_use_twitter_en/") \
    .setInputCols(["sentence_embeddings"]) \
    .setOutputCol("sentiment") \


keywords = YakeKeywordExtraction() \
    .setInputCols(["token"]) \
    .setOutputCol("keywords") \
    .setThreshold(0.6) \
    .setMinNGrams(2) \
    .setNKeywords(10)


nlpPipeline = Pipeline(
      stages = [
          documentAssembler,
          tokenizer,

        sentence,
        sentenceEmbeddings,
        universalSentenceEmbeddings,
          sentiment,
          keywords,
      ]) \
      .fit(dfB)
result = nlpPipeline.transform(dfB)


query = result.writeStream \
    .format("console") \
    .start()

query.awaitTermination()
