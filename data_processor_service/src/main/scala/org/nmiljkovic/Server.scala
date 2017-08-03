package org.nmiljkovic

import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx

class MyServer extends ScalaVerticle {
  override def start(): Unit = {
    vertx
      .createHttpServer()
      .requestHandler(_.response()
        .putHeader("content-type", "text/plain")
        .end("Hello from Vert.x"))
      .listen(8085)
  }
}


object Hello extends App {
  val server = Vertx.vertx()
  val serverName = classOf[MyServer].getName

  server.deployVerticle(s"scala:$serverName")
  val dps = new DataProcessorService()
}
