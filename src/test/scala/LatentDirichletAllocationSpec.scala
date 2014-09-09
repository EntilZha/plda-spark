package org.lda

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import breeze.linalg._
import org.scalatest.FunSuite
import org.scalatest.Matchers


class LatentDirichletAllocationSpec extends FunSuite with Matchers {
  test("seq_op correctly adds WordTopic to existing matrix") {
    var m = DenseMatrix.zeros[Int](2, 2)
    var wt = new WordTopic(0, 0)
    m = LatentDirichletAllocation.seq_op(m, wt)
    m.toString() should equal (DenseMatrix((1, 0), (0, 0)).toString())
    wt = new WordTopic(0, 1)
    m = LatentDirichletAllocation.seq_op(m, wt)
    m.toString() should equal (DenseMatrix((1, 1), (0, 0)).toString())
    wt = new WordTopic(1, 0)
    m = LatentDirichletAllocation.seq_op(m, wt)
    m.toString() should equal (DenseMatrix((1, 1), (1, 0)).toString())
  }
  test("compute_top_words gives correct DenseMatrix given a RDD of Documents with WordTopics") {
    val sc = new SparkContext("local", "test")
    val data = sc.parallelize(List(
      (0, Array(new WordTopic(0, 0)).toIterable), (0, Array(new WordTopic(0, 0)).toIterable),
      (0, Array(new WordTopic(1, 0)).toIterable), (0, Array(new WordTopic(2, 0)).toIterable),
      (0, Array(new WordTopic(1, 1)).toIterable), (0, Array(new WordTopic(2, 2)).toIterable)
    ))
    val result = LatentDirichletAllocation.compute_c_word(data, 3, 3)
    result.toString() should equal (DenseMatrix((2, 0, 0),
                                                (1, 1, 0),
                                                (1, 0, 1)).toString())
  }
}