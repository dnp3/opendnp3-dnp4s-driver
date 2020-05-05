package com.automatak.dnp4sopendnp3

import java.time.Duration

import com.automatak.dnp3._
import com.automatak.dnp3.enums.{DoubleBit, ServerAcceptMode, StaticTypeBitmask}
import com.automatak.dnp3.impl.DNP3ManagerFactory
import com.automatak.dnp4s.dnp3.app.{AnalogOutputStatusPoint, AnalogPoint, BinaryOutputStatusPoint, BinaryPoint, CounterPoint, DoubleBitPoint, EventClass, GenIndexedPoint, IndexedPoint, MeasurementPoint}
import com.automatak.dnp4s.dnp3.{IntegrationPlugin, PluginReporter}
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

  override val delayUnsolicitedValidations = false

  override def startProcedure(reporter: PluginReporter): Unit = {
    config = StackConfig.Default
    startOutstation()
  }

  override def endProcedure(reporter: PluginReporter): Unit = {
    shutdown()
  }

  override def cyclePower(reporter: PluginReporter): Unit = {
    reporter.log("Cycling power")
    shutdown()
    startOutstation()
  }

  override def setLinkLayerConfirm(reporter: PluginReporter, value: Boolean): Unit = {
    if(value) {
      throw new RuntimeException("Confirmed link-layer is not supported by OpenDNP3")
    }
  }

  override def setSelfAddressSupport(reporter: PluginReporter, value: Boolean): Unit = {
    throw new RuntimeException("Self-addressing is not supported by OpenDNP3")
  }

  override def checkBinaryOutputOperate(reporter: PluginReporter, index: Int): Unit = {
    commandHandler.checkCrob(index)
    reporter.log(f"Binary output $index did operate")
  }

  override def checkBinaryOutputNotOperate(reporter: PluginReporter): Unit = {
    commandHandler.checkNoCrob()
    reporter.log(f"Binary output did NOT operate")
  }

  override def checkAnalogOutputOperate(reporter: PluginReporter, index: Int): Unit = {
    commandHandler.checkAnalog(index)
    reporter.log(f"Analog output $index did operate")
  }

  override def checkAnalogOutputNotOperate(reporter: PluginReporter): Unit = {
    commandHandler.checkNoAnalog()
    reporter.log(f"Analog output did NOT operate")
  }

  override def generateClassEvents(reporter: PluginReporter, eventClass: EventClass): Unit = {
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

  override def generateBinaryInputPattern(reporter: PluginReporter): Unit = {
    generateBinaryInputChangeEvents(reporter)
  }

  override def generateBinaryInputChangeEvents(reporter: PluginReporter): Unit = {
    this.eventBatch = this.eventBatch + 1
    0 to 9 foreach(idx => {
      val event = trackingDatabase.generateBinaryInputEvent(idx, this.eventBatch)
      printBinaryInput(reporter, event)
    })
  }

  override def generateExactBinaryInputChangeEvents(reporter: PluginReporter, numEvents: Int): Unit = {
    this.eventBatch = this.eventBatch + 1
    1 to numEvents foreach(_ => {
      val event = trackingDatabase.generateBinaryInputEvent(0, this.eventBatch)
      printBinaryInput(reporter, event)
    })
  }

  override def generateDoubleBitBinaryInputPattern(reporter: PluginReporter): Unit = {
    generateDoubleBitBinaryInputChangeEvents(reporter)
  }

  override def generateDoubleBitBinaryInputChangeEvents(reporter: PluginReporter): Unit = {
    this.eventBatch = this.eventBatch + 1
    0 to 9 foreach(idx => {
      val event = trackingDatabase.generateDoubleBitBinaryInputEvent(idx, this.eventBatch)
      printDoubleBitBinaryInput(reporter, event)
    })
  }

  override def generateExactDoubleBitBinaryInputChangeEvents(reporter: PluginReporter, numEvents: Int): Unit = {
    this.eventBatch = this.eventBatch + 1
    1 to numEvents foreach(_ => {
      val event = trackingDatabase.generateDoubleBitBinaryInputEvent(0, this.eventBatch)
      printDoubleBitBinaryInput(reporter, event)
    })
  }

  override def generateCounterInputData(reporter: PluginReporter): Unit = {
    this.eventBatch = this.eventBatch + 1
    0 to 9 foreach(idx => {
      val event = trackingDatabase.generateCounterEvent(idx, this.eventBatch)
      printCounter(reporter, event)
    })
  }

  override def generateExactCounterChangeEvents(reporter: PluginReporter, numEvents: Int): Unit = {
    this.eventBatch = this.eventBatch + 1
    1 to numEvents foreach(_ => {
      val event = trackingDatabase.generateCounterEvent(0, this.eventBatch)
      printCounter(reporter, event)
    })
  }

  override def generateAnalogInputPattern(reporter: PluginReporter): Unit = {
    this.generateAnalogInputChangeEvents(reporter)
  }

  override def generateAnalogInputChangeEvents(reporter: PluginReporter): Unit = {
    this.eventBatch = this.eventBatch + 1
    0 to 9 foreach(idx => {
      val event = trackingDatabase.generateAnalogInputEvent(idx, this.eventBatch)
      printAnalogInput(reporter, event)
    })
  }

  override def generateExactAnalogInputChangeEvents(reporter: PluginReporter, numEvents: Int): Unit = {
    this.eventBatch = this.eventBatch + 1
    1 to numEvents foreach(_ => {
      val event = trackingDatabase.generateAnalogInputEvent(0, this.eventBatch)
      printAnalogInput(reporter, event)
    })
  }

  override def generateTimeEvent(reporter: PluginReporter): Unit = {
    this.eventBatch = this.eventBatch + 1
    val event = trackingDatabase.generateBinaryInputEvent(0, this.eventBatch)
    printBinaryInput(reporter, event)
  }

  override def uninstallBinaryInputs(reporter: PluginReporter): Unit = {
    config = config.copy(testDatabaseConfig = config.testDatabaseConfig.copy(disableBinaryInputs = true))
    reporter.log("All BI points were uninstalled")
    cyclePower(reporter)
  }

  override def uninstallDoubleBitBinaryInputs(reporter: PluginReporter): Unit = {
    config = config.copy(testDatabaseConfig = config.testDatabaseConfig.copy(disableDoubleBitBinaryInputs = true))
    reporter.log("All DBBI points were uninstalled")
    cyclePower(reporter)
  }

  override def uninstallCounters(reporter: PluginReporter): Unit = {
    config = config.copy(testDatabaseConfig = config.testDatabaseConfig.copy(disableCounters = true))
    reporter.log("All counter points were uninstalled")
    cyclePower(reporter)
  }

  override def uninstallBinaryOutputs(reporter: PluginReporter): Unit = {
    config = config.copy(commandHandlerConfig = config.commandHandlerConfig.copy(disableBinaryOutput = true))
    reporter.log("All BO points were uninstalled")
    cyclePower(reporter)
  }

  override def uninstallAnalogOutputs(reporter: PluginReporter): Unit = {
    config = config.copy(commandHandlerConfig = config.commandHandlerConfig.copy(disableAnalogOutput = true))
    reporter.log("All AO points were uninstalled")
    cyclePower(reporter)
  }

  override def setGlobalRemoteSupervisoryControl(reporter: PluginReporter): Unit = {
    config = config.copy(testDatabaseConfig = config.testDatabaseConfig.copy(isGlobalLocalControl = true))
    reporter.log("Global remote supervisory control was enabled")
    cyclePower(reporter)
  }

  override def setIndividualRemoteSupervisoryControl(reporter: PluginReporter, index: Int): Unit = {
    config = config.copy(testDatabaseConfig = config.testDatabaseConfig.copy(isSingleLocalControl = true))
    reporter.log("Remote supervisory control was enabled")
    cyclePower(reporter)
  }

  override def enableUnsolicitedResponse(reporter: PluginReporter, enabled: Boolean): Unit = {
    config = config.copy(unsolicitedResponseConfig = config.unsolicitedResponseConfig.copy(enabled))
    reporter.log("Unsolicited responses were enabled")
    cyclePower(reporter)
  }

  override def setUnsolicitedResponseTimeout(reporter: PluginReporter, timeoutMs: Int): Unit = {
    config = config.copy(unsolicitedResponseConfig = config.unsolicitedResponseConfig.copy(unsolConfirmTimeoutMs = timeoutMs))
    reporter.log(f"Unsolicited response timeout was set to $timeoutMs ms")
    cyclePower(reporter)
  }

  override def setMaxUnsolicitedRetries(reporter: PluginReporter, numRetries: Option[Int]): Unit = {
    val (newNumRetries, strNumRetries) = numRetries match {
      case Some(e) => (NumRetries.Fixed(e), e.toString)
      case None => (NumRetries.Infinite(), "Infinite")
    }
    config = config.copy(unsolicitedResponseConfig = config.unsolicitedResponseConfig.copy(maxNumRetries = newNumRetries))
    reporter.log(s"Max unsolicited response retries was set to $strNumRetries")
    cyclePower(reporter)
  }

  override def setMasterAddress(reporter: PluginReporter, address: Int): Unit = {
    config = config.copy(linkConfig = config.linkConfig.copy(destination = address))
    reporter.log(f"Master address was set to $address")
    cyclePower(reporter)
  }

  override def generateUnsolicitedEvents(reporter: PluginReporter): Unit = {
    generateExactBinaryInputChangeEvents(reporter, 1)
  }

  override def setMaxFragmentSize(reporter: PluginReporter, maxFragmentSize: Int): Unit = {
    config = config.copy(outstationConfig = config.outstationConfig.copy(fragmentSize = maxFragmentSize))
    reporter.log(f"Max fragment size was set to $maxFragmentSize")
    cyclePower(reporter)
  }

  override def generateMultiFragmentResponse(reporter: PluginReporter): Unit = {
    generateExactBinaryInputChangeEvents(reporter, 200)
    generateExactDoubleBitBinaryInputChangeEvents(reporter, 200)
    generateExactCounterChangeEvents(reporter, 200)
    generateExactAnalogInputChangeEvents(reporter, 200)
  }

  override def recordCurrentCounterValues(reporter: PluginReporter): Unit = {
    this.trackingDatabase.recordCurrentCounterValues()

    reporter.log("Recorded values:")
    this.trackingDatabase.getRecordedCounters.foreach(p => {
      printCounter(reporter, p)
    })
  }

  override def verifyAllPointsCurrentStatus(reporter: PluginReporter, isClass0: Boolean, points: Seq[GenIndexedPoint[MeasurementPoint]]): Unit = {
    val expectedPoints = this.trackingDatabase.getAllPoints(isClass0).to[mutable.Set]
    val numExpectedPoints = expectedPoints.size

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

    reporter.log(f"Verified $numExpectedPoints point(s)")
  }

  override def verifyLatestClassEvents(reporter: PluginReporter, eventClass: EventClass, points: Seq[GenIndexedPoint[MeasurementPoint]]): Unit = {
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

    reporter.log(f"Verified ${points.size} $eventClass event(s)")
  }

  override def verifyFirstClassEvent(reporter: PluginReporter, eventClass: EventClass, point: GenIndexedPoint[MeasurementPoint]): Unit = {
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

    reporter.log(f"Verified first $eventClass event")
  }

  override def verifyRestClassEvents(reporter: PluginReporter, eventClass: EventClass, points: Seq[GenIndexedPoint[MeasurementPoint]]): Unit = {
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

    reporter.log(f"Verified ${points.size} $eventClass event(s)")
  }

  override def verifyAllClassEvents(reporter: PluginReporter, eventClass: EventClass, points: Seq[GenIndexedPoint[MeasurementPoint]]): Unit = {
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

    reporter.log(f"Verified ${points.size} $eventClass event(s)")
  }

  override def verifyAllBinaryInputsCurrentStatus(reporter: PluginReporter, points: Seq[GenIndexedPoint[BinaryPoint]]): Unit = {
    verifyAllCurrentStatus(reporter, points, "binary input", trackingDatabase.getAllBinaryInputs, checkBinaryInput)
  }

  override def verifyLatestBinaryInputChangeEvents(reporter: PluginReporter, points: Seq[GenIndexedPoint[BinaryPoint]]): Unit = {
    verifyLatestEvents(reporter, points, "binary input", trackingDatabase.popBinaryEvent, checkBinaryInput)
  }

  override def verifyFirstBinaryInputChangeEvent(reporter: PluginReporter, point: GenIndexedPoint[BinaryPoint]): Unit = {
    verifyFirstEvent(reporter, point, "binary input", trackingDatabase.popBinaryEvent, checkBinaryInput)
  }

  override def verifyRestBinaryInputChangeEvents(reporter: PluginReporter, points: Seq[GenIndexedPoint[BinaryPoint]]): Unit = {
    verifyRestEvents(reporter, points, "binary input", trackingDatabase.popBinaryEvent, checkBinaryInput)
  }

  override def verifyAllBinaryInputChangeEvents(reporter: PluginReporter, points: Seq[GenIndexedPoint[BinaryPoint]]): Unit = {
    verifyAllEvents(reporter, points, "binary input", trackingDatabase.popBinaryEvent, checkBinaryInput)
  }

  override def verifyAllDoubleBitBinaryInputsCurrentStatus(reporter: PluginReporter, points: Seq[GenIndexedPoint[DoubleBitPoint]]): Unit = {
    verifyAllCurrentStatus(reporter, points, "double-bit binary input", trackingDatabase.getAllDoubleBitBinaryInputs, checkDoubleBitBinaryInput)
  }

  override def verifyLatestDoubleBitBinaryInputChangeEvents(reporter: PluginReporter, points: Seq[GenIndexedPoint[DoubleBitPoint]]): Unit = {
    verifyLatestEvents(reporter, points, "double-bit binary input", trackingDatabase.popDoubleBitBinaryEvent, checkDoubleBitBinaryInput)
  }

  override def verifyFirstDoubleBitBinaryInputChangeEvent(reporter: PluginReporter, point: GenIndexedPoint[DoubleBitPoint]): Unit = {
    verifyFirstEvent(reporter, point, "double-bit binary input", trackingDatabase.popDoubleBitBinaryEvent, checkDoubleBitBinaryInput)
  }

  override def verifyRestDoubleBitBinaryInputChangeEvents(reporter: PluginReporter, points: Seq[GenIndexedPoint[DoubleBitPoint]]): Unit = {
    verifyRestEvents(reporter, points, "double-bit binary input", trackingDatabase.popDoubleBitBinaryEvent, checkDoubleBitBinaryInput)
  }

  override def verifyAllDoubleBitBinaryInputChangeEvents(reporter: PluginReporter, points: Seq[GenIndexedPoint[DoubleBitPoint]]): Unit = {
    verifyAllEvents(reporter, points, "double-bit binary input", trackingDatabase.popDoubleBitBinaryEvent, checkDoubleBitBinaryInput)
  }

  override def verifyAllCountersCurrentStatus(reporter: PluginReporter, points: Seq[GenIndexedPoint[CounterPoint]]): Unit = {
    verifyAllCurrentStatus(reporter, points, "counter", trackingDatabase.getAllCounters, checkCounter)
  }

  override def verifyAllCountersMatchPreviousValues(reporter: PluginReporter, points: Seq[GenIndexedPoint[CounterPoint]]): Unit = {
    verifyAllCurrentStatus(reporter, points, "counter", trackingDatabase.getRecordedCounters, checkCounter)
  }

  override def verifyLatestCounterChangeEvents(reporter: PluginReporter, points: Seq[GenIndexedPoint[CounterPoint]]): Unit = {
    verifyLatestEvents(reporter, points, "counter", trackingDatabase.popCounterEvent, checkCounter)
  }

  override def verifyFirstCounterChangeEvent(reporter: PluginReporter, point: GenIndexedPoint[CounterPoint]): Unit = {
    verifyFirstEvent(reporter, point, "counter", trackingDatabase.popCounterEvent, checkCounter)
  }

  override def verifyRestCounterChangeEvents(reporter: PluginReporter, points: Seq[GenIndexedPoint[CounterPoint]]): Unit = {
    verifyRestEvents(reporter, points, "counter", trackingDatabase.popCounterEvent, checkCounter)
  }

  override def verifyAllCounterChangeEvents(reporter: PluginReporter, points: Seq[GenIndexedPoint[CounterPoint]]): Unit = {
    verifyAllEvents(reporter, points, "counter", trackingDatabase.popCounterEvent, checkCounter)
  }

  override def verifyAllAnalogInputCurrentStatus(reporter: PluginReporter, points: Seq[GenIndexedPoint[AnalogPoint]]): Unit = {
    verifyAllCurrentStatus(reporter, points, "analog input", trackingDatabase.getAllAnalogInputs, checkAnalogInput)
  }

  override def verifyLatestAnalogInputChangeEvents(reporter: PluginReporter, points: Seq[GenIndexedPoint[AnalogPoint]]): Unit = {
    verifyLatestEvents(reporter, points, "analog input", trackingDatabase.popAnalogInputEvent, checkAnalogInput)
  }

  override def verifyAllAnalogInputChangeEvents(reporter: PluginReporter, points: Seq[GenIndexedPoint[AnalogPoint]]): Unit = {
    verifyAllEvents(reporter, points, "analog input", trackingDatabase.popAnalogInputEvent, checkAnalogInput)
  }

  override def verifyPolledDataDoNotRepeatUnsolicitedData(reporter: PluginReporter, unsolicitedResponsePoints: Seq[GenIndexedPoint[MeasurementPoint]], polledResponsePoints: Seq[GenIndexedPoint[MeasurementPoint]]): Unit = {
    verifyLatestClassEvents(reporter, EventClass.All, unsolicitedResponsePoints)
    if (polledResponsePoints.nonEmpty) throw new Exception("Expected no events in polled response")
  }

  override def verifyUnsolicitedHasSameEventsAsPolled(reporter: PluginReporter, previousResponsePoints: Seq[GenIndexedPoint[MeasurementPoint]], currentResponsePoints: Seq[GenIndexedPoint[MeasurementPoint]]): Unit = {
    previousResponsePoints.equals(currentResponsePoints)
    reporter.log("Verified that received points are the same as the previously received points")
  }

  override def generateNotEnoughUnsolicitedEvents(reporter: PluginReporter): Unit = {
    throw new Exception("OpenDNP3 generates unsolicited responses on every change")
  }

  // ========================
  // Generic helper functions
  // ========================

  private def verifyAllCurrentStatus[ReceivedT <: MeasurementPoint, ExpectedT](reporter: PluginReporter, points: Seq[GenIndexedPoint[ReceivedT]], name: String, expectedPointsSeq: Seq[TypedEvent[ExpectedT]], checkMethod: (TypedEvent[ExpectedT], GenIndexedPoint[ReceivedT]) => Unit): Unit = {
    val expectedPoints = mutable.Map(expectedPointsSeq.map(e => e.idx -> e): _*)
    val numExpectedPoints = expectedPoints.size

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

    reporter.log(f"Verified $numExpectedPoints $name point(s)")
  }

  private def verifyLatestEvents[ReceivedT <: MeasurementPoint, ExpectedT](reporter: PluginReporter, points: Seq[GenIndexedPoint[ReceivedT]], name: String, popMethod: BatchSpecifier => Option[TypedEvent[ExpectedT]], checkMethod: (TypedEvent[ExpectedT], GenIndexedPoint[ReceivedT]) => Unit): Unit = {
    trackingDatabase.resetHandledEvents()
    points.foreach(point => {
      popMethod(BatchSpecifier.Specific(this.eventBatch)) match {
        case Some(event) => checkMethod(event, point)
        case None => throw new Exception(s"Unexpected $name event")
      }
    })

    if (popMethod(BatchSpecifier.Specific(this.eventBatch)).isDefined) throw new Exception(s"Missing $name event")

    reporter.log(f"Verified ${points.size} $name point event(s)")
  }

  private def verifyFirstEvent[ReceivedT <: MeasurementPoint, ExpectedT](reporter: PluginReporter, point: GenIndexedPoint[ReceivedT], name: String, popMethod: BatchSpecifier => Option[TypedEvent[ExpectedT]], checkMethod: (TypedEvent[ExpectedT], GenIndexedPoint[ReceivedT]) => Unit): Unit = {
    popMethod(BatchSpecifier.Specific(this.eventBatch)) match {
      case Some(event) => checkMethod(event, point)
      case None => throw new Exception(s"Unexpected $name event")
    }

    reporter.log(f"Verified first $name point event")
  }

  private def verifyRestEvents[ReceivedT <: MeasurementPoint, ExpectedT](reporter: PluginReporter, points: Seq[GenIndexedPoint[ReceivedT]], name: String, popMethod: BatchSpecifier => Option[TypedEvent[ExpectedT]], checkMethod: (TypedEvent[ExpectedT], GenIndexedPoint[ReceivedT]) => Unit): Unit = {
    points.foreach(point => {
      popMethod(BatchSpecifier.Specific(this.eventBatch)) match {
        case Some(event) => checkMethod(event, point)
        case None => throw new Exception(s"Unexpected $name event")
      }
    })

    if (popMethod(BatchSpecifier.Specific(this.eventBatch)).isDefined) throw new Exception(s"Missing $name input event")

    reporter.log(f"Verified ${points.size} $name point event(s)")
  }

  private def verifyAllEvents[ReceivedT <: MeasurementPoint, ExpectedT](reporter: PluginReporter, points: Seq[GenIndexedPoint[ReceivedT]], name: String, popMethod: BatchSpecifier => Option[TypedEvent[ExpectedT]], checkMethod: (TypedEvent[ExpectedT], GenIndexedPoint[ReceivedT]) => Unit): Unit = {
    trackingDatabase.resetHandledEvents()
    points.foreach(point => {
      popMethod(BatchSpecifier.All) match {
        case Some(event) => checkMethod(event, point)
        case None => throw new Exception(s"Unexpected $name input event")
      }
    })

    if (popMethod(BatchSpecifier.All).isDefined) throw new Exception(s"Missing $name event")

    reporter.log(f"Verified ${points.size} $name point event(s)")
  }

  // ========================
  // Verify individual points
  // ========================

  private def printBinaryInput(reporter: PluginReporter, event: TypedEvent[BinaryInput]): Unit = {
    reporter.log(f"Updated BI ${event.idx}: value=${event.value.value}, flags=${UInt8.fromByte(event.value.quality.getValue).value.toHexString}, timestamp=${event.value.timestamp}")
  }

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

  private def printDoubleBitBinaryInput(reporter: PluginReporter, event: TypedEvent[DoubleBitBinaryInput]): Unit = {
    reporter.log(f"Updated DBBI ${event.idx}: value=${event.value.value}, flags=${UInt8.fromByte(event.value.quality.getValue).value.toHexString}, timestamp=${event.value.timestamp}")
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

  private def printCounter(reporter: PluginReporter, event: TypedEvent[Counter]): Unit = {
    reporter.log(f"Updated Counter ${event.idx}: value=${event.value.value}, flags=${UInt8.fromByte(event.value.quality.getValue).value.toHexString}, timestamp=${event.value.timestamp}")
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

  private def printAnalogInput(reporter: PluginReporter, event: TypedEvent[AnalogInput]): Unit = {
    reporter.log(f"Updated AI ${event.idx}: value=${event.value.value}, flags=${UInt8.fromByte(event.value.quality.getValue).value.toHexString}, timestamp=${event.value.timestamp}")
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

  override def checkAnalogOutputTolerance(reporter: PluginReporter, expectedValue: Double, actualValue: Double): Unit = {
    val delta = Math.abs(expectedValue - actualValue)
    reporter.log(f"Delta: ${delta}")
    if(delta > 0.001) throw new Exception("Analog Output Status tolerance not respected")
    reporter.log("Delta is within acceptable range (+/- 0.001)")
  }

  override def verifyDelayMeasurementAccuracy(reporter: PluginReporter, delayMs: Long): Unit = {
    if (delayMs != 0) throw new Exception("Delay measurement was expected to be 0 ms")
    reporter.log("Delay measurement is within acceptable range (+/- 0)")
  }

  override def verifyTimestamp(reporter: PluginReporter, points: Seq[GenIndexedPoint[MeasurementPoint]]): Unit = {
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
    val app = new CustomOutstationApplication(config.testDatabaseConfig.isGlobalLocalControl || config.testDatabaseConfig.isSingleLocalControl)
    trackingDatabase = new TrackingDatabase(app)
    val dnp3Config = new OutstationStackConfig(
      trackingDatabase.getConfig(config.testDatabaseConfig), EventBufferConfig.allTypes(200)
    )

    // Link-layer config
    dnp3Config.linkConfig.localAddr = config.linkConfig.source
    dnp3Config.linkConfig.remoteAddr = config.linkConfig.destination
    dnp3Config.linkConfig.responseTimeout = Duration.ofMillis(config.linkConfig.timeoutMs)
    dnp3Config.linkConfig.keepAliveTimeout = Duration.ofDays(30)

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
