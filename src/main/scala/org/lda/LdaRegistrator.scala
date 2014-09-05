package org.lda

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import breeze.linalg.{DenseMatrix, DenseVector}
import scala.collection.TraversableOnce
import org.apache.spark.broadcast.Broadcast

/**
 * Created by pedro on 5/6/14.
 */
class LdaRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[LatentDirichletAllocation])
    kryo.register(classOf[WordTopic])
    kryo.register(classOf[DenseVector[Int]])
    kryo.register(classOf[DenseMatrix[Int]])
    kryo.register(classOf[DenseMatrix[Double]])
    kryo.register(classOf[DenseVector[Double]])
    kryo.register(classOf[Map[String, Int]])
    kryo.register(classOf[List[DenseMatrix[Int]]])
    kryo.register(classOf[TraversableOnce[(Int, WordTopic)]])
    kryo.register(classOf[Broadcast[Map[String, Int]]])
    kryo.register(classOf[Int])
    kryo.register(classOf[Double])
    kryo.register(classOf[String])
    kryo.register(classOf[Seq[WordTopic]])
    kryo.register(classOf[(Int, Int)])
  }
}
