package com.automatak.dnp4sopendnp3

object Main {
  def main(args: Array[String]): Unit = {
    com.automatak.dnp4s.conformance.Main.run(args, new OpenDNP3IntegrationPlugin())
  }
}
