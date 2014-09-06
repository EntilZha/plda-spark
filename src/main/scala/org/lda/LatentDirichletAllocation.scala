package org.lda

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import breeze.linalg._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import org.apache.log4j.{Level, Logger}
import scala.io.Source
import scala.collection.TraversableOnce
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel

object LatentDirichletAllocation {
  val SPARK_HOME = "/root/spark"
  val JAR_FILE = "target/scala-2.10/org/lda-assembly-1.0.jar"
  val MASTER = "local"
  val FILE = "/Users/pedro/Documents/Code/plda-spark/data/science.txt"
  //val MASTER = Source.fromFile("/root/spark-ec2/cluster-url").mkString.trim
  //val FILE = "/root/lda/data/data" + data_file_size.toString + "MB.txt"
  val ITERATIONS = 10
  val K = 4
}

class LatentDirichletAllocation() extends Serializable {
  def generate_vocab_id_lookup(vocab:Array[String]) : Map[String, Int] = {
    var vocab_lookup:Map[String, Int] = Map()
    for (i <- 0 until vocab.length) {
      vocab_lookup += (vocab(i) -> i)
    }
    return vocab_lookup
  }

  // Compute a matrix holding the topic mixture for each document
  def compute_topic_mixture(vocab:Array[String], docs:Array[(Int, Seq[WordTopic])]) : List[DenseVector[Int]] = {
    val results = ListBuffer[DenseVector[Int]]()
    for (doc <- docs) {
      val v = DenseVector.zeros[Int](LatentDirichletAllocation.K)
      for (wt <- doc._2) {
        v(wt.topic) += 1
      }
      results.append(v)
    }
    return results.toList
  }

  // Compute a matrix holding the top n words for each topic
  def compute_top_words(vocab:Array[String], doc_words:Array[(Int, Seq[WordTopic])], n:Int=10) : List[DenseVector[Int]] = {
    val results = ListBuffer
    return null
  }


  def run(data_file_size: Int, tasks: Int) {
    val K = LatentDirichletAllocation.K
    Logger.getLogger("spark").setLevel(Level.WARN)
    val conf = new SparkConf()
                  .setMaster(LatentDirichletAllocation.MASTER)
                  .setAppName("LDA")
                  .setJars(Seq(LatentDirichletAllocation.JAR_FILE))
                  .setSparkHome(LatentDirichletAllocation.SPARK_HOME)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "org.lda.LdaRegistrator")
    val spark_context = new SparkContext(conf)
    val data = spark_context.textFile(LatentDirichletAllocation.FILE, tasks)
    //Run through all words to generate a vocab, vocab size, and vocab lookup
    val vocab = data.flatMap(line => line.split(" ")).distinct().collect()
    val vocab_lookup = spark_context.broadcast(generate_vocab_id_lookup(vocab))
    val V = vocab_lookup.value.size

    //Parse individual documents into (d0, List[WordTopic(word, random topic)])
    var grouped_documents_with_wt = data.flatMap({(line) =>
      val words = line.split(" ")
      words.map(w => {
        (line.hashCode(), new WordTopic(vocab_lookup.value(w), Random.nextInt(K)))
      })
    }).groupByKey()
    //BEGIN LOOP HERE
    for (x <- 1 to LatentDirichletAllocation.ITERATIONS)
    {
      val c_word = grouped_documents_with_wt.flatMap(kv => {
        kv._2.map(wt => {
          (wt.word, wt.topic)
        })
      }).groupByKey().mapPartitions(partition => {
        val v = DenseVector.zeros[Int](K)
        partition.map(kv => {
          v :*= 0
          kv._2.foreach(t => {
            v(t) += 1
          })
          (kv._1, v)
        })
      }).mapPartitions(partition => {
        val m = DenseMatrix.zeros[Int](V, K)
        partition.foreach(kv => {
          val key = kv._1
          val v = kv._2
          m(key, ::) :+= v.t
        })
        Array[DenseMatrix[Int]](m).iterator
      }).reduce(_ + _)
      val c_word_sum:DenseVector[Int] = sum(c_word(::, *)).toDenseVector
      val c_doc_d = DenseVector.zeros[Int](K)

      val ALPHA = .1
      val BETA = .1
      val posterior_range = DenseVector((0 to K - 1).toArray)
      val posterior = DenseVector.zeros[Double](K)
      val new_grouped_documents_with_wt = grouped_documents_with_wt.mapPartitions(partition => {
        def find_k(p:Double, posterior:DenseVector[Double]): Int = {
          posterior_range.foreach(e => {
            if (p < posterior(e)) {
              return e
            }
          })
          return K - 1
        }
        partition.map(doc => {
          c_doc_d :*= 0
          doc._2.foreach(wt => {
            c_doc_d(wt.topic) += 1
          })
          posterior :*= 0.0
          //Now with c_doc for a given doc computed, do gibbs and emit new WordTopics
          val wts_new = doc._2.map(wt => {
            c_doc_d(wt.topic) -= 1
            c_word_sum(wt.topic) -= 1
            c_word(wt.word, wt.topic) -= 1
            var posterior_sum = 0.0
            posterior_range.foreach(k => {
              posterior_sum += (ALPHA + c_doc_d(k)) * (c_word(wt.word, k) + BETA) / (V * BETA + c_word_sum(k))
              posterior(k) = posterior_sum
            })
            posterior :*= 1 / posterior_sum
            wt.topic = find_k(Random.nextDouble(), posterior)
            c_doc_d(wt.topic) += 1
            c_word(wt.word, wt.topic) += 1
            c_word_sum(wt.topic) += 1
            wt
          })
          (doc._1, wts_new)
        })
      }).cache()
      new_grouped_documents_with_wt.count()
      grouped_documents_with_wt.unpersist(blocking = false)
      grouped_documents_with_wt = null
      grouped_documents_with_wt = new_grouped_documents_with_wt
    }
    //END LOOP HERE
    val results = grouped_documents_with_wt.collect()
    val topic_mixture = compute_topic_mixture(vocab, results)
    for (e <- topic_mixture) {
      println(e.toString())
    }
    val top_words = compute_top_words(vocab, results)
    spark_context.stop()
  }
}
