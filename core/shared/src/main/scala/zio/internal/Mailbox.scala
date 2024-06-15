package zio.internal

import java.util.concurrent.atomic.AtomicReference

final class Mailbox[A](private var read: Mailbox.Node[A]) extends AtomicReference(read) {

  def this() =
    this(Mailbox.Node(null.asInstanceOf[A]))

  def add(data: A) = {
    val next = Mailbox.Node(data)
    getAndSet(next).lazySet(next)
  }

  def isEmpty(): Boolean =
    null == read.get()

  def nonEmpty(): Boolean =
    null != read.get()

  def poll(): A = {
    val next = read.getAcquire()

    if (null == next) return null.asInstanceOf[A]

    val data = next.data
    read = next
    next.data = null.asInstanceOf[A]
    data
  }
}

object Mailbox {

  final class Node[A](var data: A) extends AtomicReference[Node[A]]

  object Node {

    def apply[A](data: A): Node[A] =
      new Node(data)

    def apply[A](data: A, next: Node[A]): Node[A] = {
      val node = new Node(data)
      node.setPlain(next)
      node
    }
  }
}
