package com.automatak.dnp4sopendnp3

import java.time.Duration

import com.automatak.dnp3._
import com.automatak.dnp3.enums.{DoubleBit, ServerAcceptMode, StaticTypeBitmask}
import com.automatak.dnp3.impl.DNP3ManagerFactory
import com.automatak.dnp4s.dnp3.app.{AnalogOutputStatusPoint, AnalogPoint, BinaryOutputStatusPoint, BinaryPoint, CounterPoint, DoubleBitPoint, EventClass, GenIndexedPoint, IndexedPoint, MeasurementPoint}
import com.automatak.dnp4s.dnp3.{IntegrationPlugin, TestReporter}
import com.automatak.dnp4s.protocol.parsing.UInt8

import scala.collection.mutable
import scala.reflect.ClassTag

class OpenDNP3IntegrationPlugin extends IntegrationPlugin {
  private var manager: DNP3Manager = null
  private var channel: Channel = null
  private var outstation: Outstation = null
  private var trackingDatabase: TrackingDatabase = null
  private var commandHandler: QueuedCommandHandler = null
  private var config = StackConfig.Default
  private var eventBatch = 0

  override def startProcedure(reporter: TestReporter): Unit = {
    config = StackConfig.Default
    startOutstation()
  }

  override def endProcedure(reporter: TestReporter): Unit = {
    shutdown()
  }

  override def cyclePower(reporter: TestReporter): Unit = {
    shutdown()
    startOutstation()
  }

  override def setLinkLayerConfirm(reporter: TestReporter, value: Boolean): Unit = {
    config = config.copy(linkConfig = config.linkConfig.copy(useConfirms = value))
    cyclePower(reporter)
  }

  override def setSelfAddressSupport(reporter: TestReporter, value: Boolean): Unit = {
    throw new RuntimeException("Self-addressing is not supported by OpenDNP3")
  }

  override def checkBinaryOutputOperate(reporter: TestReporter, index: Int): Unit = {
    commandHandler.checkCrob(index)
  }

  override def checkBinaryOutputNotOperate(reporter: TestReporter): Unit = {
    commandHandler.checkNoCrob()
  }

  override def checkAnalogOutputOperate(reporter: TestReporter, index: Int): Unit = {
    commandHandler.checkAnalog(index)
  }

  override def checkAnalogOutputNotOperate(reporter: TestReporter): Unit = {
    commandHandler.checkNoAnalog()
  }

  override def generateClassEvents(reporter: TestReporter, eventClass: EventClass): Unit = {
    eventClass match {
      case EventClass.Class1 => generateBinaryInputChangeEvents(reporter)
      case EventClass.Class2 => generateCounterInputData(reporter)
      case EventClass.Class3 => generateAnalogInputChangeEvents(reporter)
      case _ => {
        generateBinaryInputChangeEvents(reporter)
        this.eventBatch = this.eventBatch - 1
        generateCounterInputData(reporter)
        this.eventBatch = this.eventBatch - 1
        generateAnalogInputChangeEvents(reporter)
      }
    }
  }

  override def generateBinaryInputPattern(reporter: TestReporter): Unit = {
    generateBinaryInputChangeEvents(reporter)
  }

  override def generateBinaryInputChangeEvents(reporter: TestReporter): Unit = {
    this.eventBatch = this.eventBatch + 1
    1 to 3 foreach(_ => {
      val event = trackingDatabase.generateBinaryInputEvent(this.eventBatch)
      reporter.info(f"Updated BI ${event.idx}: value=${event.value.value}, flags=${UInt8.fromByte(event.value.quality.getValue).value.toHexString}, timestamp=${event.value.timestamp}")
    })
  }

  override def generateExactBinaryInputChangeEvents(reporter: TestReporter, numEvents: Int): Unit = {
    this.eventBatch = this.eventBatch + 1
    1 to numEvents foreach(_ => {
      val event = trackingDatabase.generateBinaryInputEvent(this.eventBatch)
      reporter.info(f"Updated BI ${event.idx}: value=${event.value.value}, flags=${UInt8.fromByte(event.value.quality.getValue).value.toHexString}, timestamp=${event.value.timestamp}")
    })
  }

  override def generateDoubleBitBinaryInputPattern(reporter: TestReporter): Unit = {
    generateDoubleBitBinaryInputChangeEvents(reporter)
  }

  override def generateDoubleBitBinaryInputChangeEvents(reporter: TestReporter): Unit = {
    this.eventBatch = this.eventBatch + 1
    1 to 3 foreach(_ => {
      val event = trackingDatabase.generateDoubleBitBinaryInputEvent(this.eventBatch)
      reporter.info(f"Updated DBBI ${event.idx}: value=${event.value.value}, flags=${UInt8.fromByte(event.value.quality.getValue).value.toHexString}, timestamp=${event.value.timestamp}")
    })
  }

  override def generateExactDoubleBitBinaryInputChangeEvents(reporter: TestReporter, numEvents: Int): Unit = {
    this.eventBatch = this.eventBatch + 1
    1 to numEvents foreach(_ => {
      val event = trackingDatabase.generateDoubleBitBinaryInputEvent(this.eventBatch)
      reporter.info(f"Updated DBBI ${event.idx}: value=${event.value.value}, flags=${UInt8.fromByte(event.value.quality.getValue).value.toHexString}, timestamp=${event.value.timestamp}")
    })
  }

  override def generateCounterInputData(reporter: TestReporter): Unit = {
    this.eventBatch = this.eventBatch + 1
    1 to 3 foreach(_ => {
      val event = trackingDatabase.generateCounterEvent(this.eventBatch)
      reporter.info(f"Updated Counter ${event.idx}: value=${event.value.value}, flags=${UInt8.fromByte(event.value.quality.getValue).value.toHexString}, timestamp=${event.value.timestamp}")
    })
  }

  override def generateExactCounterChangeEvents(reporter: TestReporter, numEvents: Int): Unit = {
    this.eventBatch = this.eventBatch + 1
    1 to numEvents foreach(_ => {
      val event = trackingDatabase.generateCounterEvent(this.eventBatch)
      reporter.info(f"Updated Counter ${event.idx}: value=${event.value.value}, flags=${UInt8.fromByte(event.value.quality.getValue).value.toHexString}, timestamp=${event.value.timestamp}")
    })
  }

  override def generateAnalogInputPattern(reporter: TestReporter): Unit = {
    this.generateAnalogInputChangeEvents(reporter)
  }

  override def generateAnalogInputChangeEvents(reporter: TestReporter): Unit = {
    this.eventBatch = this.eventBatch + 1
    1 to 3 foreach(_ => {
      val event = trackingDatabase.generateAnalogInputEvent(this.eventBatch)
      reporter.info(f"Updated AI ${event.idx}: value=${event.value.value}, flags=${UInt8.fromByte(event.value.quality.getValue).value.toHexString}, timestamp=${event.value.timestamp}")
    })
  }

  override def generateExactAnalogInputChangeEvents(reporter: TestReporter, numEvents: Int): Unit = {
    this.eventBatch = this.eventBatch + 1
    1 to numEvents foreach(_ => {
      val event = trackingDatabase.generateAnalogInputEvent(this.eventBatch)
      reporter.info(f"Updated AI ${event.idx}: value=${event.value.value}, flags=${UInt8.fromByte(event.value.quality.getValue).value.toHexString}, timestamp=${event.value.timestamp}")
    })
  }

  override def generateTimeEvent(reporter: TestReporter): Unit = {
    this.eventBatch = this.eventBatch + 1
    val event = trackingDatabase.generateBinaryInputEvent(this.eventBatch)
    reporter.info(f"Updated BI ${event.idx}: value=${event.value.value}, flags=${UInt8.fromByte(event.value.quality.getValue).value.toHexString}, timestamp=${event.value.timestamp}")
  }

  override def uninstallBinaryInputs(reporter: TestReporter): Unit = {
    config = config.copy(testDatabaseConfig = config.testDatabaseConfig.copy(disableBinaryInputs = true))
    cyclePower(reporter)
  }

  override def uninstallDoubleBitBinaryInputs(reporter: TestReporter): Unit = {
    config = config.copy(testDatabaseConfig = config.testDatabaseConfig.copy(disableDoubleBitBinaryInputs = true))
    cyclePower(reporter)
  }

  override def uninstallCounters(reporter: TestReporter): Unit = {
    config = config.copy(testDatabaseConfig = config.testDatabaseConfig.copy(disableCounters = true))
    cyclePower(reporter)
  }

  override def uninstallBinaryOutputs(reporter: TestReporter): Unit = {
    config = config.copy(commandHandlerConfig = config.commandHandlerConfig.copy(disableBinaryOutput = true))
    cyclePower(reporter)
  }

  override def uninstallAnalogOutputs(reporter: TestReporter): Unit = {
    config = config.copy(commandHandlerConfig = config.commandHandlerConfig.copy(disableAnalogOutput = true))
    cyclePower(reporter)
  }

  override def setGlobalRemoteSupervisoryControl(reporter: TestReporter): Unit = {
    config = config.copy(testDatabaseConfig = config.testDatabaseConfig.copy(isLocalControl = true))
    cyclePower(reporter)
  }

  override def setIndividualRemoteSupervisoryControl(reporter: TestReporter, index: Int): Unit = {
    config = config.copy(testDatabaseConfig = config.testDatabaseConfig.copy(isLocalControl = true))
    cyclePower(reporter)
  }

  override def enableUnsolicitedResponse(reporter: TestReporter, enabled: Boolean): Unit = {
    config = config.copy(unsolicitedResponseConfig = config.unsolicitedResponseConfig.copy(enabled))
    cyclePower(reporter)
  }

  override def setUnsolicitedResponseTimeout(reporter: TestReporter, timeoutMs: Int): Unit = {
    config = config.copy(unsolicitedResponseConfig = config.unsolicitedResponseConfig.copy(unsolConfirmTimeoutMs = timeoutMs))
    cyclePower(reporter)
  }

  override def setMaxUnsolicitedRetries(reporter: TestReporter, numRetries: Option[Int]): Unit = {
    val newNumRetries = numRetries match {
      case Some(e) => NumRetries.Fixed(e)
      case None => NumRetries.Infinite()
    }
    config = config.copy(unsolicitedResponseConfig = config.unsolicitedResponseConfig.copy(maxNumRetries = newNumRetries))
    cyclePower(reporter)
  }

  override def setMasterAddress(reporter: TestReporter, address: Int): Unit = {
    config = config.copy(linkConfig = config.linkConfig.copy(destination = address))
    cyclePower(reporter)
  }

  override def generateUnsolicitedEvents(reporter: TestReporter): Unit = {
    generateExactBinaryInputChangeEvents(reporter, 1)
  }

  override def setMaxFragmentSize(reporter: TestReporter, maxFragmentSize: Int): Unit = {
    config = config.copy(outstationConfig = config.outstationConfig.copy(fragmentSize = maxFragmentSize))
    cyclePower(reporter)
  }

  override def generateMultiFragmentResponse(reporter: TestReporter): Unit = {
    generateExactBinaryInputChangeEvents(reporter, 200)
    generateExactDoubleBitBinaryInputChangeEvents(reporter, 200)
    generateExactCounterChangeEvents(reporter, 200)
    generateExactAnalogInputChangeEvents(reporter, 200)
  }

  override def recordCurrentCounterValues(reporter: TestReporter): Unit = {
    this.trackingDatabase.recordCurrentCounterValues()
  }

  override def verifyAllPointsCurrentStatus(reporter: TestReporter, isClass0: Boolean, points: Seq[GenIndexedPoint[MeasurementPoint]]): Unit = {
    val expectedPoints = this.trackingDatabase.getAllPoints(isClass0).to[mutable.Set]

    def findPoint[T](idx: Int)(implicit tag: ClassTag[T]): TypedEvent[T] = {
      val point = expectedPoints.find {
        case event @ TypedEvent(_: T) if event.idx == idx => true
        case _ => false
      }

      point match {
        case Some(p) => {
          expectedPoints.remove(p)
          p.asInstanceOf[TypedEvent[T]]
        }
        case None => throw new Exception("Received unexpected point")
      }
    }

    points.foreach(receivedPoint => {
      receivedPoint.point match {
        case _: BinaryPoint => {
          val expectedPoint = findPoint[BinaryInput](receivedPoint.idx)
          checkBinaryInput(expectedPoint, receivedPoint.asInstanceOf[GenIndexedPoint[BinaryPoint]])
        }
        case _: DoubleBitPoint => {
          val expectedPoint = findPoint[DoubleBitBinaryInput](receivedPoint.idx)
          checkDoubleBitBinaryInput(expectedPoint, receivedPoint.asInstanceOf[GenIndexedPoint[DoubleBitPoint]])
        }
        case p: CounterPoint if !p.isFrozen => {
          val expectedPoint = findPoint[Counter](receivedPoint.idx)
          checkCounter(expectedPoint, receivedPoint.asInstanceOf[GenIndexedPoint[CounterPoint]])
        }
        case p: CounterPoint if p.isFrozen => {
          val expectedPoint = findPoint[FrozenCounter](receivedPoint.idx)
          checkFrozenCounter(expectedPoint, receivedPoint.asInstanceOf[GenIndexedPoint[CounterPoint]])
        }
        case _: AnalogPoint => {
          val expectedPoint = findPoint[AnalogInput](receivedPoint.idx)
          checkAnalogInput(expectedPoint, receivedPoint.asInstanceOf[GenIndexedPoint[AnalogPoint]])
        }
        case _: BinaryOutputStatusPoint => {
          val expectedPoint = findPoint[BinaryOutputStatus](receivedPoint.idx)
          checkBinaryOutput(expectedPoint, receivedPoint.asInstanceOf[GenIndexedPoint[BinaryOutputStatusPoint]])
        }
        case _: AnalogOutputStatusPoint => {
          val expectedPoint = findPoint[AnalogOutputStatus](receivedPoint.idx)
          checkAnalogOutput(expectedPoint, receivedPoint.asInstanceOf[GenIndexedPoint[AnalogOutputStatusPoint]])
        }
      }
    })

    if(expectedPoints.nonEmpty) throw new Exception("DUT did not report all the points")
  }

  override def verifyLatestClassEvents(reporter: TestReporter, eventClass: EventClass, points: Seq[GenIndexedPoint[MeasurementPoint]]): Unit = {
    trackingDatabase.resetHandledEvents()

    def getTypedEvent[T <: MeasurementPoint](event: GenIndexedPoint[MeasurementPoint])(implicit tag: ClassTag[T]): GenIndexedPoint[T] = {
      event.point match {
        case _: T => event.asInstanceOf[GenIndexedPoint[T]]
        case _ => throw new Exception("Received unexpected event type")
      }
    }

    points.foreach(receivedEvent => {
      val expectedEvent = trackingDatabase.popEvent(BatchSpecifier.Specific(this.eventBatch), eventClass).getOrElse(throw new Exception("Unexpected event"))
      expectedEvent match {
        case TypedEvent(_: BinaryInput) => checkBinaryInput(expectedEvent.asInstanceOf[TypedEvent[BinaryInput]], getTypedEvent(receivedEvent))
        case TypedEvent(_: DoubleBitBinaryInput) => checkDoubleBitBinaryInput(expectedEvent.asInstanceOf[TypedEvent[DoubleBitBinaryInput]], getTypedEvent(receivedEvent))
        case TypedEvent(_: Counter) => checkCounter(expectedEvent.asInstanceOf[TypedEvent[Counter]], getTypedEvent(receivedEvent))
        case TypedEvent(_: FrozenCounter) => checkFrozenCounter(expectedEvent.asInstanceOf[TypedEvent[FrozenCounter]], getTypedEvent(receivedEvent))
        case TypedEvent(_: AnalogInput) => checkAnalogInput(expectedEvent.asInstanceOf[TypedEvent[AnalogInput]], getTypedEvent(receivedEvent))
        case TypedEvent(_: BinaryOutputStatus) => checkBinaryOutput(expectedEvent.asInstanceOf[TypedEvent[BinaryOutputStatus]], getTypedEvent(receivedEvent))
        case TypedEvent(_: AnalogOutputStatus) => checkAnalogOutput(expectedEvent.asInstanceOf[TypedEvent[AnalogOutputStatus]], getTypedEvent(receivedEvent))
      }
    })

    if (trackingDatabase.popEvent(BatchSpecifier.Specific(this.eventBatch), eventClass).isDefined) throw new Exception("Missing event")
  }

  override def verifyFirstClassEvent(reporter: TestReporter, eventClass: EventClass, point: GenIndexedPoint[MeasurementPoint]): Unit = {
    def getTypedEvent[T <: MeasurementPoint](event: GenIndexedPoint[MeasurementPoint])(implicit tag: ClassTag[T]): GenIndexedPoint[T] = {
      event.point match {
        case _: T => event.asInstanceOf[GenIndexedPoint[T]]
        case _ => throw new Exception("Received unexpected event type")
      }
    }

    val expectedEvent = trackingDatabase.popEvent(BatchSpecifier.Specific(this.eventBatch), eventClass).getOrElse(throw new Exception("Unexpected event"))
    expectedEvent match {
      case TypedEvent(_: BinaryInput) => checkBinaryInput(expectedEvent.asInstanceOf[TypedEvent[BinaryInput]], getTypedEvent(point))
      case TypedEvent(_: DoubleBitBinaryInput) => checkDoubleBitBinaryInput(expectedEvent.asInstanceOf[TypedEvent[DoubleBitBinaryInput]], getTypedEvent(point))
      case TypedEvent(_: Counter) => checkCounter(expectedEvent.asInstanceOf[TypedEvent[Counter]], getTypedEvent(point))
      case TypedEvent(_: FrozenCounter) => checkFrozenCounter(expectedEvent.asInstanceOf[TypedEvent[FrozenCounter]], getTypedEvent(point))
      case TypedEvent(_: AnalogInput) => checkAnalogInput(expectedEvent.asInstanceOf[TypedEvent[AnalogInput]], getTypedEvent(point))
      case TypedEvent(_: BinaryOutputStatus) => checkBinaryOutput(expectedEvent.asInstanceOf[TypedEvent[BinaryOutputStatus]], getTypedEvent(point))
      case TypedEvent(_: AnalogOutputStatus) => checkAnalogOutput(expectedEvent.asInstanceOf[TypedEvent[AnalogOutputStatus]], getTypedEvent(point))
    }
  }

  override def verifyRestClassEvents(reporter: TestReporter, eventClass: EventClass, points: Seq[GenIndexedPoint[MeasurementPoint]]): Unit = {
    def getTypedEvent[T <: MeasurementPoint](event: GenIndexedPoint[MeasurementPoint])(implicit tag: ClassTag[T]): GenIndexedPoint[T] = {
      event.point match {
        case _: T => event.asInstanceOf[GenIndexedPoint[T]]
        case _ => throw new Exception("Received unexpected event type")
      }
    }

    points.foreach(receivedEvent => {
      val expectedEvent = trackingDatabase.popEvent(BatchSpecifier.Specific(this.eventBatch), eventClass).getOrElse(throw new Exception("Unexpected event"))
      expectedEvent match {
        case TypedEvent(_: BinaryInput) => checkBinaryInput(expectedEvent.asInstanceOf[TypedEvent[BinaryInput]], getTypedEvent(receivedEvent))
        case TypedEvent(_: DoubleBitBinaryInput) => checkDoubleBitBinaryInput(expectedEvent.asInstanceOf[TypedEvent[DoubleBitBinaryInput]], getTypedEvent(receivedEvent))
        case TypedEvent(_: Counter) => checkCounter(expectedEvent.asInstanceOf[TypedEvent[Counter]], getTypedEvent(receivedEvent))
        case TypedEvent(_: FrozenCounter) => checkFrozenCounter(expectedEvent.asInstanceOf[TypedEvent[FrozenCounter]], getTypedEvent(receivedEvent))
        case TypedEvent(_: AnalogInput) => checkAnalogInput(expectedEvent.asInstanceOf[TypedEvent[AnalogInput]], getTypedEvent(receivedEvent))
        case TypedEvent(_: BinaryOutputStatus) => checkBinaryOutput(expectedEvent.asInstanceOf[TypedEvent[BinaryOutputStatus]], getTypedEvent(receivedEvent))
        case TypedEvent(_: AnalogOutputStatus) => checkAnalogOutput(expectedEvent.asInstanceOf[TypedEvent[AnalogOutputStatus]], getTypedEvent(receivedEvent))
      }
    })

    if (trackingDatabase.popEvent(BatchSpecifier.Specific(this.eventBatch), eventClass).isDefined) throw new Exception("Missing event")
  }

  override def verifyAllClassEvents(reporter: TestReporter, eventClass: EventClass, points: Seq[GenIndexedPoint[MeasurementPoint]]): Unit = {
    trackingDatabase.resetHandledEvents()

    def getTypedEvent[T <: MeasurementPoint](event: GenIndexedPoint[MeasurementPoint])(implicit tag: ClassTag[T]): GenIndexedPoint[T] = {
      event.point match {
        case _: T => event.asInstanceOf[GenIndexedPoint[T]]
        case _ => throw new Exception("Received unexpected event type")
      }
    }

    points.foreach(receivedEvent => {
      val expectedEvent = trackingDatabase.popEvent(BatchSpecifier.All, eventClass).getOrElse(throw new Exception("Unexpected event"))
      expectedEvent match {
        case TypedEvent(_: BinaryInput) => checkBinaryInput(expectedEvent.asInstanceOf[TypedEvent[BinaryInput]], getTypedEvent(receivedEvent))
        case TypedEvent(_: DoubleBitBinaryInput) => checkDoubleBitBinaryInput(expectedEvent.asInstanceOf[TypedEvent[DoubleBitBinaryInput]], getTypedEvent(receivedEvent))
        case TypedEvent(_: Counter) => checkCounter(expectedEvent.asInstanceOf[TypedEvent[Counter]], getTypedEvent(receivedEvent))
        case TypedEvent(_: FrozenCounter) => checkFrozenCounter(expectedEvent.asInstanceOf[TypedEvent[FrozenCounter]], getTypedEvent(receivedEvent))
        case TypedEvent(_: AnalogInput) => checkAnalogInput(expectedEvent.asInstanceOf[TypedEvent[AnalogInput]], getTypedEvent(receivedEvent))
        case TypedEvent(_: BinaryOutputStatus) => checkBinaryOutput(expectedEvent.asInstanceOf[TypedEvent[BinaryOutputStatus]], getTypedEvent(receivedEvent))
        case TypedEvent(_: AnalogOutputStatus) => checkAnalogOutput(expectedEvent.asInstanceOf[TypedEvent[AnalogOutputStatus]], getTypedEvent(receivedEvent))
      }
    })

    if (trackingDatabase.popEvent(BatchSpecifier.All, eventClass).isDefined) throw new Exception("Missing event")
  }

  override def verifyAllBinaryInputsCurrentStatus(reporter: TestReporter, points: Seq[GenIndexedPoint[BinaryPoint]]): Unit = {
    verifyAllCurrentStatus(points, "binary input", trackingDatabase.getAllBinaryInputs, checkBinaryInput)
  }

  override def verifyLatestBinaryInputChangeEvents(reporter: TestReporter, points: Seq[GenIndexedPoint[BinaryPoint]]): Unit = {
    verifyLatestEvents(points, "binary input", trackingDatabase.popBinaryEvent, checkBinaryInput)
  }

  override def verifyFirstBinaryInputChangeEvent(reporter: TestReporter, point: GenIndexedPoint[BinaryPoint]): Unit = {
    verifyFirstEvent(point, "binary input", trackingDatabase.popBinaryEvent, checkBinaryInput)
  }

  override def verifyRestBinaryInputChangeEvents(reporter: TestReporter, points: Seq[GenIndexedPoint[BinaryPoint]]): Unit = {
    verifyRestEvents(points, "binary input", trackingDatabase.popBinaryEvent, checkBinaryInput)
  }

  override def verifyAllBinaryInputChangeEvents(reporter: TestReporter, points: Seq[GenIndexedPoint[BinaryPoint]]): Unit = {
    verifyAllEvents(points, "binary input", trackingDatabase.popBinaryEvent, checkBinaryInput)
  }

  override def verifyAllDoubleBitBinaryInputsCurrentStatus(reporter: TestReporter, points: Seq[GenIndexedPoint[DoubleBitPoint]]): Unit = {
    verifyAllCurrentStatus(points, "double-bit binary input", trackingDatabase.getAllDoubleBitBinaryInputs, checkDoubleBitBinaryInput)
  }

  override def verifyLatestDoubleBitBinaryInputChangeEvents(reporter: TestReporter, points: Seq[GenIndexedPoint[DoubleBitPoint]]): Unit = {
    verifyLatestEvents(points, "double-bit binary input", trackingDatabase.popDoubleBitBinaryEvent, checkDoubleBitBinaryInput)
  }

  override def verifyFirstDoubleBitBinaryInputChangeEvent(reporter: TestReporter, point: GenIndexedPoint[DoubleBitPoint]): Unit = {
    verifyFirstEvent(point, "double-bit binary input", trackingDatabase.popDoubleBitBinaryEvent, checkDoubleBitBinaryInput)
  }

  override def verifyRestDoubleBitBinaryInputChangeEvents(reporter: TestReporter, points: Seq[GenIndexedPoint[DoubleBitPoint]]): Unit = {
    verifyRestEvents(points, "double-bit binary input", trackingDatabase.popDoubleBitBinaryEvent, checkDoubleBitBinaryInput)
  }

  override def verifyAllDoubleBitBinaryInputChangeEvents(reporter: TestReporter, points: Seq[GenIndexedPoint[DoubleBitPoint]]): Unit = {
    verifyAllEvents(points, "double-bit binary input", trackingDatabase.popDoubleBitBinaryEvent, checkDoubleBitBinaryInput)
  }

  override def verifyAllCountersCurrentStatus(reporter: TestReporter, points: Seq[GenIndexedPoint[CounterPoint]]): Unit = {
    verifyAllCurrentStatus(points, "counter", trackingDatabase.getAllCounters, checkCounter)
  }

  override def verifyAllCountersMatchPreviousValues(reporter: TestReporter, points: Seq[GenIndexedPoint[CounterPoint]]): Unit = {
    verifyAllCurrentStatus(points, "counter", trackingDatabase.getRecordedCounters, checkCounter)
  }

  override def verifyLatestCounterChangeEvents(reporter: TestReporter, points: Seq[GenIndexedPoint[CounterPoint]]): Unit = {
    verifyLatestEvents(points, "counter", trackingDatabase.popCounterEvent, checkCounter)
  }

  override def verifyFirstCounterChangeEvent(reporter: TestReporter, point: GenIndexedPoint[CounterPoint]): Unit = {
    verifyFirstEvent(point, "counter", trackingDatabase.popCounterEvent, checkCounter)
  }

  override def verifyRestCounterChangeEvents(reporter: TestReporter, points: Seq[GenIndexedPoint[CounterPoint]]): Unit = {
    verifyRestEvents(points, "counter", trackingDatabase.popCounterEvent, checkCounter)
  }

  override def verifyAllCounterChangeEvents(reporter: TestReporter, points: Seq[GenIndexedPoint[CounterPoint]]): Unit = {
    verifyAllEvents(points, "counter", trackingDatabase.popCounterEvent, checkCounter)
  }

  override def verifyAllAnalogInputCurrentStatus(reporter: TestReporter, points: Seq[GenIndexedPoint[AnalogPoint]]): Unit = {
    verifyAllCurrentStatus(points, "analog input", trackingDatabase.getAllAnalogInputs, checkAnalogInput)
  }

  override def verifyLatestAnalogInputChangeEvents(reporter: TestReporter, points: Seq[GenIndexedPoint[AnalogPoint]]): Unit = {
    verifyLatestEvents(points, "analog input", trackingDatabase.popAnalogInputEvent, checkAnalogInput)
  }

  override def verifyAllAnalogInputChangeEvents(reporter: TestReporter, points: Seq[GenIndexedPoint[AnalogPoint]]): Unit = {
    verifyAllEvents(points, "analog input", trackingDatabase.popAnalogInputEvent, checkAnalogInput)
  }

  override def verifyPolledDataDoNotRepeatUnsolicitedData(reporter: TestReporter, unsolicitedResponsePoints: Seq[GenIndexedPoint[MeasurementPoint]], polledResponsePoints: Seq[GenIndexedPoint[MeasurementPoint]]): Unit = {
    verifyLatestClassEvents(reporter, EventClass.All, unsolicitedResponsePoints)
    if (polledResponsePoints.nonEmpty) throw new Exception("Expected no events in polled response")
  }

  override def verifyUnsolicitedHasSameEventsAsPolled(reporter: TestReporter, previousResponsePoints: Seq[GenIndexedPoint[MeasurementPoint]], currentResponsePoints: Seq[GenIndexedPoint[MeasurementPoint]]): Unit = {
    previousResponsePoints.equals(currentResponsePoints)
  }

  override def generateNotEnoughUnsolicitedEvents(reporter: TestReporter): Unit = {
    throw new Exception("OpenDNP3 generates unsolicited responses on every change")
  }

  // ========================
  // Generic helper functions
  // ========================

  private def verifyAllCurrentStatus[ReceivedT <: MeasurementPoint, ExpectedT](points: Seq[GenIndexedPoint[ReceivedT]], name: String, expectedPointsSeq: Seq[TypedEvent[ExpectedT]], checkMethod: (TypedEvent[ExpectedT], GenIndexedPoint[ReceivedT]) => Unit): Unit = {
    val expectedPoints = mutable.Map(expectedPointsSeq.map(e => e.idx -> e): _*)

    points.foreach(actualPoint => {
      expectedPoints.get(actualPoint.idx) match {
        case Some(expectedPoint) => {
          expectedPoints.remove(actualPoint.idx)
          checkMethod(expectedPoint, actualPoint)
        }
        case None => throw new Exception(s"Unexpected $name")
      }
    })

    if (expectedPoints.nonEmpty) throw new Exception(s"Missing $name point")
  }

  private def verifyLatestEvents[ReceivedT <: MeasurementPoint, ExpectedT](points: Seq[GenIndexedPoint[ReceivedT]], name: String, popMethod: BatchSpecifier => Option[TypedEvent[ExpectedT]], checkMethod: (TypedEvent[ExpectedT], GenIndexedPoint[ReceivedT]) => Unit): Unit = {
    trackingDatabase.resetHandledEvents()
    points.foreach(point => {
      popMethod(BatchSpecifier.Specific(this.eventBatch)) match {
        case Some(event) => checkMethod(event, point)
        case None => throw new Exception(s"Unexpected $name event")
      }
    })

    if (popMethod(BatchSpecifier.Specific(this.eventBatch)).isDefined) throw new Exception(s"Missing $name event")
  }

  private def verifyFirstEvent[ReceivedT <: MeasurementPoint, ExpectedT](point: GenIndexedPoint[ReceivedT], name: String, popMethod: BatchSpecifier => Option[TypedEvent[ExpectedT]], checkMethod: (TypedEvent[ExpectedT], GenIndexedPoint[ReceivedT]) => Unit): Unit = {
    popMethod(BatchSpecifier.Specific(this.eventBatch)) match {
      case Some(event) => checkMethod(event, point)
      case None => throw new Exception(s"Unexpected $name event")
    }
  }

  private def verifyRestEvents[ReceivedT <: MeasurementPoint, ExpectedT](points: Seq[GenIndexedPoint[ReceivedT]], name: String, popMethod: BatchSpecifier => Option[TypedEvent[ExpectedT]], checkMethod: (TypedEvent[ExpectedT], GenIndexedPoint[ReceivedT]) => Unit): Unit = {
    points.foreach(point => {
      popMethod(BatchSpecifier.Specific(this.eventBatch)) match {
        case Some(event) => checkMethod(event, point)
        case None => throw new Exception(s"Unexpected $name event")
      }
    })

    if (popMethod(BatchSpecifier.Specific(this.eventBatch)).isDefined) throw new Exception(s"Missing $name input event")
  }

  private def verifyAllEvents[ReceivedT <: MeasurementPoint, ExpectedT](points: Seq[GenIndexedPoint[ReceivedT]], name: String, popMethod: BatchSpecifier => Option[TypedEvent[ExpectedT]], checkMethod: (TypedEvent[ExpectedT], GenIndexedPoint[ReceivedT]) => Unit): Unit = {
    trackingDatabase.resetHandledEvents()
    points.foreach(point => {
      popMethod(BatchSpecifier.All) match {
        case Some(event) => checkMethod(event, point)
        case None => throw new Exception(s"Unexpected $name input event")
      }
    })

    if (popMethod(BatchSpecifier.All).isDefined) throw new Exception(s"Missing $name event")
  }

  // ========================
  // Verify individual points
  // ========================

  private def checkBinaryInput(expectedValue: TypedEvent[BinaryInput], receivedValue: GenIndexedPoint[BinaryPoint]): Unit = {
    // Check index
    if (expectedValue.idx != receivedValue.idx) throw new Exception("Unknown binary point event reported")

    // Check value
    if (expectedValue.value.value != receivedValue.point.value) throw new Exception("Binary did not report proper value")

    // Check flags (if present)
    receivedValue.point.flags match {
      case Some(flags) => if (flags != expectedValue.value.quality.getValue) throw new Exception("Binary did not report proper flags")
      case None =>
    }

    // Check timestamp (if present)
    receivedValue.point.timestamp match {
      case Some(timestamp) => if (timestamp.value != expectedValue.value.timestamp.msSinceEpoch) throw new Exception("Binary did not report proper timestamp")
      case None =>
    }
  }

  private def checkDoubleBitBinaryInput(expectedValue: TypedEvent[DoubleBitBinaryInput], receivedValue: GenIndexedPoint[DoubleBitPoint]): Unit = {
    // Check index
    if (expectedValue.idx != receivedValue.idx) throw new Exception("Unknown double-bit binary point event reported")

    // Check value
    if ((expectedValue.value.value == DoubleBit.DETERMINED_OFF && receivedValue.point.value != com.automatak.dnp4s.dnp3.app.DoubleBit.DeterminedOff) ||
      (expectedValue.value.value == DoubleBit.DETERMINED_ON && receivedValue.point.value != com.automatak.dnp4s.dnp3.app.DoubleBit.DeterminedOn) ||
      (expectedValue.value.value == DoubleBit.INDETERMINATE && receivedValue.point.value != com.automatak.dnp4s.dnp3.app.DoubleBit.Indeterminate) ||
      (expectedValue.value.value == DoubleBit.INTERMEDIATE && receivedValue.point.value != com.automatak.dnp4s.dnp3.app.DoubleBit.Intermediate)) {
      throw new Exception("Double-bit binary did not report proper value")
    }

    // Check flags (if present)
    receivedValue.point.flags match {
      case Some(flags) => if (flags != expectedValue.value.quality.getValue) throw new Exception("Double-bit binary did not report proper flags")
      case None =>
    }

    // Check timestamp (if present)
    receivedValue.point.timestamp match {
      case Some(timestamp) => if (timestamp.value != expectedValue.value.timestamp.msSinceEpoch) throw new Exception("Double-bit binary did not report proper timestamp")
      case None =>
    }
  }

  private def checkCounter(expectedValue: TypedEvent[Counter], receivedValue: GenIndexedPoint[CounterPoint]): Unit = {
    // Check index
    if (expectedValue.idx != receivedValue.idx) throw new Exception("Unknown counter point reported")

    // Check value
    if (expectedValue.value.value != receivedValue.point.value) throw new Exception("Counter did not report proper value")

    // Check flags (if present)
    receivedValue.point.flags match {
      case Some(flags) => if (flags != expectedValue.value.quality.getValue) throw new Exception("Counter did not report proper flags")
      case None =>
    }

    // Check timestamp (if present)
    receivedValue.point.timestamp match {
      case Some(timestamp) => if (timestamp.value != expectedValue.value.timestamp.msSinceEpoch) throw new Exception("Counter did not report proper timestamp")
      case None =>
    }
  }

  private def checkFrozenCounter(expectedValue: TypedEvent[FrozenCounter], receivedValue: GenIndexedPoint[CounterPoint]): Unit = {
    // Check index
    if (expectedValue.idx != receivedValue.idx) throw new Exception("Unknown frozen counter point reported")

    // Check value
    if (expectedValue.value.value != receivedValue.point.value) throw new Exception("Frozen counter did not report proper value")

    // Check flags (if present)
    receivedValue.point.flags match {
      case Some(flags) => if (flags != expectedValue.value.quality.getValue) throw new Exception("Frozen counter did not report proper flags")
      case None =>
    }

    // Check timestamp (if present)
    receivedValue.point.timestamp match {
      case Some(timestamp) => if (timestamp.value != expectedValue.value.timestamp.msSinceEpoch) throw new Exception("Frozen counter did not report proper timestamp")
      case None =>
    }
  }

  private def checkAnalogInput(expectedValue: TypedEvent[AnalogInput], receivedValue: GenIndexedPoint[AnalogPoint]): Unit = {
    // Check index
    if (expectedValue.idx != receivedValue.idx) throw new Exception("Unknown analog input point reported")

    // Check value
    if (expectedValue.value.value != receivedValue.point.value) throw new Exception("Analog input did not report proper value")

    // Check flags (if present)
    receivedValue.point.flags match {
      case Some(flags) => if (flags != expectedValue.value.quality.getValue) throw new Exception("Analog input did not report proper flags")
      case None =>
    }

    // Check timestamp (if present)
    receivedValue.point.timestamp match {
      case Some(timestamp) => if (timestamp.value != expectedValue.value.timestamp.msSinceEpoch) throw new Exception("Analog input did not report proper timestamp")
      case None =>
    }
  }

  private def checkBinaryOutput(expectedValue: TypedEvent[BinaryOutputStatus], receivedValue: GenIndexedPoint[BinaryOutputStatusPoint]): Unit = {
    // Check index
    if (expectedValue.idx != receivedValue.idx) throw new Exception("Unknown binary output point reported")

    // Check value
    if (expectedValue.value.value != receivedValue.point.value) throw new Exception("Binary output did not report proper value")

    // Check flags (if present)
    receivedValue.point.flags match {
      case Some(flags) => if (flags != expectedValue.value.quality.getValue) throw new Exception("Binary output did not report proper flags")
      case None =>
    }

    // Check timestamp (if present)
    receivedValue.point.timestamp match {
      case Some(timestamp) => if (timestamp.value != expectedValue.value.timestamp.msSinceEpoch) throw new Exception("Binary output did not report proper timestamp")
      case None =>
    }
  }

  private def checkAnalogOutput(expectedValue: TypedEvent[AnalogOutputStatus], receivedValue: GenIndexedPoint[AnalogOutputStatusPoint]): Unit = {
    // Check index
    if (expectedValue.idx != receivedValue.idx) throw new Exception("Unknown analog output point reported")

    // Check value
    if (expectedValue.value.value != receivedValue.point.value) throw new Exception("Analog output did not report proper value")

    // Check flags (if present)
    receivedValue.point.flags match {
      case Some(flags) => if (flags != expectedValue.value.quality.getValue) throw new Exception("Analog output did not report proper flags")
      case None =>
    }

    // Check timestamp (if present)
    receivedValue.point.timestamp match {
      case Some(timestamp) => if (timestamp.value != expectedValue.value.timestamp.msSinceEpoch) throw new Exception("Analog output did not report proper timestamp")
      case None =>
    }
  }

  override def checkAnalogOutputTolerance(reporter: TestReporter, expectedValue: Double, actualValue: Double): Unit = {
    val delta = Math.abs(expectedValue - actualValue)
    reporter.info(f"Delta: ${delta}"
    )
    if(delta > 0.001) throw new Exception("Analog Output Status tolerance not respected")
  }

  override def verifyDelayMeasurementAccuracy(reporter: TestReporter, delayMs: Long): Unit = {
    if (delayMs != 0) throw new Exception("Delay measurement was expected to be 0 ms")
  }

  override def verifyTimestamp(reporter: TestReporter, points: Seq[GenIndexedPoint[MeasurementPoint]]): Unit = {
    verifyAllClassEvents(reporter, EventClass.All, points)
  }

  private def startOutstation(): Unit = {
    // Create manager
    manager = DNP3ManagerFactory.createManager(1, new CustomLogHandler)

    // Create channel
    channel = manager.addTCPServer(
      "channel",
      LogMasks.APP_COMMS,
      ServerAcceptMode.CloseExisting,
      new IPEndpoint(config.tcpConfig.address, config.tcpConfig.port),
      new CustomChannelListener
    )

    // Create config
    val app = new CustomOutstationApplication(config.testDatabaseConfig.isLocalControl)
    trackingDatabase = new TrackingDatabase(app)
    val dnp3Config = new OutstationStackConfig(
      trackingDatabase.getConfig(config.testDatabaseConfig), EventBufferConfig.allTypes(200)
    )

    // Link-layer config
    dnp3Config.linkConfig.localAddr = config.linkConfig.source
    dnp3Config.linkConfig.remoteAddr = config.linkConfig.destination
    dnp3Config.linkConfig.useConfirms = config.linkConfig.useConfirms
    dnp3Config.linkConfig.numRetry = config.linkConfig.numRetry
    dnp3Config.linkConfig.responseTimeout = Duration.ofMillis(config.linkConfig.timeoutMs)

    // Outstation config
    dnp3Config.outstationConfig.solConfirmTimeout = Duration.ofMillis(config.outstationConfig.responseTimeoutMs)
    dnp3Config.outstationConfig.selectTimeout = Duration.ofMillis(config.outstationConfig.selectTimeoutMs)
    dnp3Config.outstationConfig.maxTxFragSize = config.outstationConfig.fragmentSize
    dnp3Config.outstationConfig.maxControlsPerRequest = config.outstationConfig.maxControlsPerRequest.toShort
    dnp3Config.outstationConfig.typesAllowedInClass0 = StaticTypeBitField.from(
      StaticTypeBitmask.BinaryInput,
      StaticTypeBitmask.Counter,
      StaticTypeBitmask.FrozenCounter,
      StaticTypeBitmask.AnalogInput,
      StaticTypeBitmask.BinaryOutputStatus,
      StaticTypeBitmask.AnalogOutputStatus
    )

    // Unsolicited responses config
    dnp3Config.outstationConfig.allowUnsolicited = config.unsolicitedResponseConfig.allowUnsolicited
    dnp3Config.outstationConfig.unsolConfirmTimeout = Duration.ofMillis(config.unsolicitedResponseConfig.unsolConfirmTimeoutMs)
    dnp3Config.outstationConfig.noDefferedReadDuringUnsolicitedNullResponse = config.unsolicitedResponseConfig.allowUnsolicited
    dnp3Config.outstationConfig.numUnsolRetries = config.unsolicitedResponseConfig.maxNumRetries

    // Command handler
    commandHandler = new QueuedCommandHandler(app, config.commandHandlerConfig.disableBinaryOutput, config.commandHandlerConfig.disableAnalogOutput)

    outstation = channel.addOutstation(
      "outstation",
      commandHandler,
      app,
      dnp3Config
    )

    trackingDatabase.init(outstation)
    commandHandler.setOutstation(outstation)

    outstation.enable()
  }

  private def shutdown(): Unit = {
    if(manager != null) {
      manager.shutdown()
    }
  }
}
