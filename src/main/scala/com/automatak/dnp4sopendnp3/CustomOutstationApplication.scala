package com.automatak.dnp4sopendnp3

import java.time.{Duration, Instant}

import com.automatak.dnp3.{ApplicationIIN, DNPTime, OutstationApplication}
import com.automatak.dnp3.enums.{AssignClassType, LinkStatus, PointClass, RestartMode, TimestampQuality}

class CustomOutstationApplication(val isLocalControl: Boolean) extends OutstationApplication {
  private val refreshRate: Duration = Duration.ofSeconds(10)
  private var lastTimestamp = Instant.MIN
  private var lastUpdate = Instant.MIN

  override def supportsWriteAbsoluteTime() = true

  override def writeAbsoluteTime(msSinceEpoch: Long): Boolean = {
    this.lastTimestamp = Instant.ofEpochMilli(msSinceEpoch)
    this.lastUpdate = Instant.now
    true
  }

  override def supportsAssignClass() = true

  override def recordClassAssignment(`type`: AssignClassType, clazz: PointClass, start: Int, stop: Int): Unit = {}

  override def getApplicationIIN: ApplicationIIN = {
    val result = ApplicationIIN.none()
    result.needTime = needsTime
    result.localControl = isLocalControl
    result
  }

  override def coldRestartSupport() = RestartMode.SUPPORTED_DELAY_FINE

  override def warmRestartSupport() = RestartMode.UNSUPPORTED

  override def coldRestart(): Int = 5000

  override def warmRestart(): Int = 65535

  override def now(): DNPTime = {
    val time = this.lastTimestamp.plus(Duration.between(this.lastUpdate, Instant.now))
    val quality = if (isTimeValid) TimestampQuality.SYNCHRONIZED else TimestampQuality.UNSYNCHRONIZED
    new DNPTime(time.toEpochMilli, quality)
  }

  override def onStateChange(value: LinkStatus): Unit = {}

  override def onKeepAliveInitiated(): Unit = {}

  override def onKeepAliveFailure(): Unit = {}

  override def onKeepAliveSuccess(): Unit = {}

  private def isTimeValid: Boolean = {
    Duration.between(this.lastUpdate, Instant.now).compareTo(refreshRate) <= 0
  }

  private def needsTime: Boolean = {
    Duration.between(this.lastUpdate, Instant.now).compareTo(refreshRate.dividedBy(2)) > 0
  }
}
