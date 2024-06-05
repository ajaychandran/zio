package zio.internal

import zio.test._
import zio.test.Assertion._
import zio.ZIO

object MailboxSpec extends ZIOSpecDefault {

  def spec =
    suite("Mailbox")(
      preserves.elements(new Mailbox(16)),
      preserves.order(new Mailbox(16)),
      suite("(without eager growth)")(
        preserves.elements(new Mailbox(16, -1)),
        preserves.order(new Mailbox(16, -1))
      )
    )

  object preserves {

    def elements(q: Mailbox[AnyRef]) =
      test("preserves elements")(
        check(Gen.chunkOf(Gen.uuid))(expected =>
          for {
            consumer <- ZIO
                          .succeed(q.poll())
                          .repeatWhile(_ == null)
                          .replicateZIO(expected.length)
                          .fork
            _ <-
              ZIO
                .withParallelism(expected.length)(
                  ZIO
                    .foreachParDiscard(expected)(s => ZIO.succeedBlocking(q.add(s)))
                )
                .fork
            actual <- consumer.join
          } yield assert(actual)(hasSameElements(expected))
        )
      )

    def order(q: Mailbox[AnyRef]) =
      test("preserves insertion order with a single producer")(
        check(Gen.chunkOf(Gen.uuid))(expected =>
          for {
            consumer <- ZIO
                          .succeed(q.poll())
                          .repeatWhile(_ == null)
                          .replicateZIO(expected.length)
                          .fork
            _      <- ZIO.succeedBlocking(expected.foreach(q.add)).fork
            actual <- consumer.join
          } yield assert(actual)(equalTo(expected))
        )
      )
  }
}
