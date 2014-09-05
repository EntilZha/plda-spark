package org.lda

class WordTopic(var word:Int, var topic:Int) extends Serializable {
  override def toString : String = "(" + word + ", " + topic + ")"
}
