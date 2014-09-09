package org.lda

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import breeze.linalg._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import org.apache.log4j.{Level, Logger}

/**
 * LatentDirichletAllocation uses spark to perform LDA in parallel.
 */
object LatentDirichletAllocation {
  val SPARK_HOME = "/root/spark"
  val JAR_FILE = "target/scala-2.10/org/lda-assembly-1.0.jar"
  val MASTER = "local"
  val FILE = "/Users/pedro/Documents/Code/plda-spark/data/science.txt"
  //val MASTER = Source.fromFile("/root/spark-ec2/cluster-url").mkString.trim
  //val FILE = "/root/lda/data/data" + data_file_size.toString + "MB.txt"
  val ITERATIONS = 10
  val K = 4

  /**
   * seq_op defines how to combine a single WordTopic into the c_word matrix
   * @param c_word matrix holding word/topic mixture
   * @param wt new WordTopic to integrate
   * @return result of adding wt into c_word
   */
  def seq_op(c_word:DenseMatrix[Int], wt:WordTopic) : DenseMatrix[Int] = {
    c_word(wt.word, wt.topic) += 1
    return c_word
  }

  def comb_op(m1:DenseMatrix[Int], m2:DenseMatrix[Int]) : DenseMatrix[Int] = {
    return m1 + m2
  }

  /**
   * compute_topic_mixture computes for each document, the topic mixture
   * TODO: Do this in parallel rather than on one machine
   * @param docs Sequence of all the documents with their WordTopics
   * @return list where each element is the topic mixture of a document
   */
  def compute_topic_mixture(docs:Array[(Int, Iterable[WordTopic])]) : List[DenseVector[Int]] = {
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

  /**
   * compute_c_word takes in tuples of (doc_id, List of WordTopics) and
   * computes the c_word matrix which tabulates for each word (as a row)
   * the number of times it was assigned to each topic.
   * @param doc_with_wts RDD of tuples with document_id as key and sequence of
   *                     WordTopics as value
   * @param V size of vocabulary
   * @param K number of topics
   * @return c_word, a DenseMatrix containing word/topic mixture
   */
  def compute_c_word(doc_with_wts:RDD[(Int, Iterable[WordTopic])], V:Int, K:Int=LatentDirichletAllocation.K) : DenseMatrix[Int] = {
    return doc_with_wts.flatMap(doc => doc._2).aggregate(
        DenseMatrix.zeros[Int](V, K))(LatentDirichletAllocation.seq_op, LatentDirichletAllocation.comb_op)
  }

  /**
   * compute_top_words finds the top n words of each topic and returns a list
   * of lists. The inner list contain a sorted sequence of strings representing
   * the most common words. The outer list indexes the different topics, where
   * each list of words belongs to that topic.
   * @param c_word matrix with rows indexing words and columns indexing topics
   * @param vocab array which acts as a lookup from the integer representation
   *              of a word to its original string representation
   * @param n number of words to find per topic
   * @return list indexed by topic containing sorted lists of top words.
   */
  def compute_top_words(c_word:DenseMatrix[Int], vocab:Array[String], n:Int) : List[List[String]] = {
    return null
  }
}

class LatentDirichletAllocation() extends Serializable {
  /**
   * generate_vocab_id_lookup creates a mapping from the original word to the
   * integer id using the array lookup from int (index) to the original word
   * @param vocab
   * @return
   */
  def generate_vocab_id_lookup(vocab:Array[String]) : Map[String, Int] = {
    var vocab_lookup:Map[String, Int] = Map()
    for (i <- 0 until vocab.length) {
      vocab_lookup += (vocab(i) -> i)
    }
    return vocab_lookup
  }

  /**
   * run starts the PLDA job.
   * @param tasks number of Spark tasks to use
   */
  def run(tasks: Int) {
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
    val topic_mixture = LatentDirichletAllocation.compute_topic_mixture(results)
    for (e <- topic_mixture) {
      println(e.toString())
    }
    val final_c_word = LatentDirichletAllocation.compute_c_word(grouped_documents_with_wt, V)
    spark_context.stop()
  }
}
