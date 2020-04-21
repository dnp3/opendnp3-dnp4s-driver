package com.automatak.dnp4sopendnp3

import com.automatak.dnp3.NumRetries

case class TcpConfig(address: String, port: Int)

case class LinkConfig(source: Int, destination: Int, timeoutMs: Int)

case class OutstationConfig(responseTimeoutMs: Int, selectTimeoutMs: Int, fragmentSize: Int, maxControlsPerRequest: Int)

case class UnsolicitedResponseConfig(allowUnsolicited: Boolean, unsolConfirmTimeoutMs: Int, maxNumRetries: NumRetries)

case class TestDatabaseConfig(disableBinaryInputs: Boolean, disableDoubleBitBinaryInputs: Boolean, disableCounters: Boolean, isLocalControl: Boolean)

case class CommandHandlerConfig(disableBinaryOutput: Boolean, disableAnalogOutput: Boolean)

case class StackConfig(tcpConfig: TcpConfig, linkConfig: LinkConfig, outstationConfig: OutstationConfig, unsolicitedResponseConfig: UnsolicitedResponseConfig, testDatabaseConfig: TestDatabaseConfig, commandHandlerConfig: CommandHandlerConfig)

object StackConfig {
  val Default = StackConfig(
    TcpConfig("127.0.0.1", 20000),
    LinkConfig(1024, 1, 1000),
    OutstationConfig(2000, 2000, 2048, 4),
    UnsolicitedResponseConfig(false, 5000, NumRetries.Infinite()),
    TestDatabaseConfig(false, false, false, false),
    CommandHandlerConfig(false, false)
  )
}
