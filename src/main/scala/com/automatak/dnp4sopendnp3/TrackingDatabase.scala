package com.automatak.dnp4sopendnp3

import com.automatak.dnp3.enums.{DoubleBit, EventMode, PointClass, StaticAnalogOutputStatusVariation, StaticAnalogVariation, StaticCounterVariation, StaticFrozenCounterVariation}
import com.automatak.dnp3.{AnalogConfig, AnalogInput, AnalogOutputStatus, AnalogOutputStatusConfig, BinaryConfig, BinaryInput, BinaryOutputStatus, BinaryOutputStatusConfig, Counter, CounterConfig, DatabaseConfig, DoubleBinaryConfig, DoubleBitBinaryInput, Flags, FrozenCounter, FrozenCounterConfig, Outstation, OutstationChangeSet}
import com.automatak.dnp4s.dnp3.app.EventClass

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

sealed trait BatchSpecifier
object BatchSpecifier {
  case object All extends BatchSpecifier
  case class Specific(val batchId: Int) extends BatchSpecifier
}

trait Event {
  def idx: Int
  def batch: Int
  def eventClass: EventClass
  def isHandled: Boolean
  def handle()
  def reset()
}

object TypedEvent {
  def unapply[T](x: TypedEvent[T]) = Some(x.value)
}
class TypedEvent[T](val idx: Int, val batch: Int, val eventClass: EventClass, val value: T) extends Event {
  var isHandled: Boolean = false

  override def handle(): Unit = {
    this.isHandled = true
  }

  override def reset(): Unit = {
    this.isHandled = false
  }
}

class TrackingDatabase(val app: CustomOutstationApplication) {
  private var outstation: Outstation = null

  private val binaryPoints = mutable.SortedMap((for (i <- 0 to 9) yield i -> new BinaryInput(false, new Flags(0x01), app.now())): _*)
  private val doubleBitPoints = mutable.SortedMap((for (i <- 0 to 9) yield i -> new DoubleBitBinaryInput(DoubleBit.DETERMINED_OFF, new Flags(0x41), app.now())): _*)
  private val counters = mutable.SortedMap((for (i <- 0 to 9) yield i -> new Counter(0, new Flags(0x01), app.now())): _*)
  private val analogInputs = mutable.SortedMap((for (i <- 0 to 9) yield i -> new AnalogInput(0.0, new Flags(0x01), app.now())): _*)

  private val binaryOutputs = mutable.SortedMap((for (i <- 0 to 19) yield i -> new BinaryOutputStatus(false, new Flags(0x01), app.now())): _*)
  private val analogOutputs = mutable.SortedMap((for (i <- 0 to 19) yield i -> new AnalogOutputStatus(0.0, new Flags(0x01), app.now())): _*)

  private val events: ArrayBuffer[Event] = ArrayBuffer()
  private var recordedCounterValues: mutable.SortedMap[Int, Counter] = mutable.SortedMap()

  def getConfig(testDatabaseConfig: TestDatabaseConfig): DatabaseConfig = {
    if(testDatabaseConfig.isGlobalLocalControl) {
      this.binaryOutputs.foreach(originalPoint => {
        this.binaryOutputs(originalPoint._1) = new BinaryOutputStatus(originalPoint._2.value, new Flags(0x11), originalPoint._2.timestamp)
      })
    }

    if(testDatabaseConfig.isSingleLocalControl) {
      val originalPoint = this.binaryOutputs(0)
      val newPoint = new BinaryOutputStatus(originalPoint.value, new Flags(0x11), originalPoint.timestamp)
      this.binaryOutputs(0) = newPoint
    }

    val config = new DatabaseConfig()

    if(!testDatabaseConfig.disableBinaryInputs) {
      val binaryConfig = new BinaryConfig()
      binaryConfig.clazz = PointClass.Class1
      this.binaryPoints.foreach(e => config.binary.put(e._1, binaryConfig))
    }

    if(!testDatabaseConfig.disableDoubleBitBinaryInputs) {
      val dbbiConfig = new DoubleBinaryConfig()
      dbbiConfig.clazz = PointClass.Class1
      this.doubleBitPoints.foreach(e => config.doubleBinary.put(e._1, dbbiConfig))
    }

    if(!testDatabaseConfig.disableCounters) {
      val counterConfig = new CounterConfig()
      counterConfig.clazz = PointClass.Class2
      counterConfig.staticVariation = StaticCounterVariation.Group20Var2

      val frozenCounterConfig = new FrozenCounterConfig()
      frozenCounterConfig.clazz = PointClass.Class1
      frozenCounterConfig.staticVariation = StaticFrozenCounterVariation.Group21Var2

      this.counters.foreach(e => {
        config.counter.put(e._1, counterConfig)
        config.frozenCounter.put(e._1, frozenCounterConfig)
      })
    }

    this.analogInputs.foreach(e => {
      val aiConfig = new AnalogConfig()
      aiConfig.clazz = PointClass.Class3
      aiConfig.staticVariation = StaticAnalogVariation.Group30Var2

      config.analog.put(e._1, aiConfig)
    })

    this.binaryOutputs.foreach(e => {
      val boConfig = new BinaryOutputStatusConfig()
      boConfig.clazz = PointClass.Class1

      config.boStatus.put(e._1, boConfig)
    })

    this.analogOutputs.foreach(e => {
      val aoConfig = new AnalogOutputStatusConfig()
      aoConfig.clazz = PointClass.Class1
      aoConfig.staticVariation = StaticAnalogOutputStatusVariation.Group40Var2

      config.aoStatus.put(e._1, aoConfig)
    })

    config
  }

  def init(outstation: Outstation): Unit = {
    this.outstation = outstation

    // Initialize the outstation values
    val changeset = new OutstationChangeSet()
    this.binaryPoints.foreach(e => changeset.update(e._2, e._1, EventMode.Suppress))
    this.doubleBitPoints.foreach(e => changeset.update(e._2, e._1, EventMode.Suppress))
    this.counters.foreach(e => changeset.update(e._2, e._1, EventMode.Suppress))
    this.counters.foreach(e => changeset.freezeCounter(e._1, false, EventMode.Suppress))
    this.analogInputs.foreach(e => changeset.update(e._2, e._1, EventMode.Suppress))
    this.binaryOutputs.foreach(e => changeset.update(e._2, e._1, EventMode.Suppress))
    this.analogOutputs.foreach(e => changeset.update(e._2, e._1, EventMode.Suppress))
    this.outstation.apply(changeset)
  }

  def generateBinaryInputEvent(idx: Int, eventBatch: Int): TypedEvent[BinaryInput] = {
    val value = !binaryPoints(idx).value
    val flags = if(value) 0x81.toByte else 0x01.toByte
    val newPoint = new BinaryInput(value, new Flags(flags), app.now())

    this.binaryPoints(idx) = newPoint
    val newEvent = new TypedEvent[BinaryInput](idx, eventBatch, EventClass.Class1, newPoint)
    this.events.append(newEvent)

    val changeset = new OutstationChangeSet()
    changeset.update(newPoint, idx)
    this.outstation.apply(changeset)

    newEvent
  }

  def generateDoubleBitBinaryInputEvent(idx: Int, eventBatch: Int): TypedEvent[DoubleBitBinaryInput] = {
    val value = if (doubleBitPoints(idx).value == DoubleBit.DETERMINED_OFF) DoubleBit.DETERMINED_ON else DoubleBit.DETERMINED_OFF
    val flags = if (value == DoubleBit.DETERMINED_OFF) 0x41.toByte else 0x81.toByte
    val newPoint = new DoubleBitBinaryInput(value, new Flags(flags), app.now())

    this.doubleBitPoints(idx) = newPoint
    val newEvent = new TypedEvent[DoubleBitBinaryInput](idx, eventBatch, EventClass.All, newPoint)
    this.events.append(newEvent)

    val changeset = new OutstationChangeSet()
    changeset.update(newPoint, idx)
    this.outstation.apply(changeset)

    newEvent
  }

  def generateCounterEvent(idx: Int, eventBatch: Int): TypedEvent[Counter] = {
    val value = counters(idx).value + 1
    val newPoint = new Counter(value, new Flags(0x01), app.now())

    this.counters(idx) = newPoint
    val newEvent = new TypedEvent[Counter](idx, eventBatch, EventClass.Class2, newPoint)
    this.events.append(newEvent)

    val changeset = new OutstationChangeSet()
    changeset.update(newPoint, idx)
    this.outstation.apply(changeset)

    newEvent
  }

  def recordCurrentCounterValues(): Unit = {
    this.recordedCounterValues = this.counters.clone()
  }

  def generateAnalogInputEvent(idx: Int, eventBatch: Int): TypedEvent[AnalogInput] = {
    val value = analogInputs(idx).value + 100.0
    val newPoint = new AnalogInput(value, new Flags(0x01), app.now())

    this.analogInputs(idx) = newPoint
    val newEvent = new TypedEvent[AnalogInput](idx, eventBatch, EventClass.Class3, newPoint)
    this.events.append(newEvent)

    val changeset = new OutstationChangeSet()
    changeset.update(newPoint, idx)
    this.outstation.apply(changeset)

    newEvent
  }

  def resetHandledEvents(): Unit = {
    this.events.foreach(e => e.reset())
  }

  def getAllPoints(isClass0: Boolean): Set[Event] = {
    (
      getAllBinaryInputs.map(e => e.asInstanceOf[Event]).toList :::
      (if (!isClass0) getAllDoubleBitBinaryInputs.map(_.asInstanceOf[Event]).toList else Nil) :::
      getAllCounters.map(_.asInstanceOf[Event]).toList :::
      getAllCounters.map(e => new TypedEvent[FrozenCounter](e.idx, e.batch, EventClass.All, new FrozenCounter(0, e.value.quality, e.value.timestamp)).asInstanceOf[Event]).toList :::
      getAllAnalogInputs.map(_.asInstanceOf[Event]).toList :::
      this.binaryOutputs.map(e => new TypedEvent[BinaryOutputStatus](e._1, 0, EventClass.All, e._2).asInstanceOf[Event]).toList :::
      this.analogOutputs.map(e => new TypedEvent[AnalogOutputStatus](e._1, 0, EventClass.All, e._2).asInstanceOf[Event]).toList
    ).toSet
  }

  def getAllEvents(batch: BatchSpecifier, eventClass: EventClass = EventClass.All): Seq[Event] = {
    this.events.filter(e => {
      batch match {
        case BatchSpecifier.All => true
        case BatchSpecifier.Specific(id) => e.batch == id
      }
    }).filter(e => {
      eventClass match {
        case EventClass.All => true
        case _ => e.eventClass == eventClass
      }
    })
  }

  def getUnhandledEvents(batch: BatchSpecifier, eventClass: EventClass): Seq[Event] = {
    getAllEvents(batch, eventClass).filter(e => !e.isHandled)
  }

  def popEvent(batch: BatchSpecifier, eventClass: EventClass): Option[Event] = {
    val head = getUnhandledEvents(batch, eventClass).headOption
    head match {
      case Some(event) => event.handle()
      case None =>
    }
    head
  }

  def getAllBinaryInputs: Seq[TypedEvent[BinaryInput]] = {
    this.binaryPoints.map(e => {
      new TypedEvent(e._1, 0, EventClass.All, e._2)
    }).toSeq
  }

  def getAllBinaryInputEvents(batch: BatchSpecifier): Seq[TypedEvent[BinaryInput]] = {
    getAllEvents(batch).flatMap {
      case event @ TypedEvent(_: BinaryInput) => Some(event.asInstanceOf[TypedEvent[BinaryInput]])
      case _ => None
    }
  }

  def getUnhandledBinaryInputEvents(batch: BatchSpecifier): Seq[TypedEvent[BinaryInput]] = {
    getAllBinaryInputEvents(batch).filter(e => !e.isHandled)
  }

  def popBinaryEvent(batch: BatchSpecifier): Option[TypedEvent[BinaryInput]] = {
    val head = getUnhandledBinaryInputEvents(batch).headOption
    head match {
      case Some(event) => event.handle()
      case None =>
    }
    head
  }

  def getAllDoubleBitBinaryInputs: Seq[TypedEvent[DoubleBitBinaryInput]] = {
    this.doubleBitPoints.map(e => {
      new TypedEvent(e._1, 0, EventClass.All, e._2)
    }).toSeq
  }

  def getAllDoubleBitBinaryInputEvents(batch: BatchSpecifier): Seq[TypedEvent[DoubleBitBinaryInput]] = {
    getAllEvents(batch).flatMap {
      case event @ TypedEvent(_: DoubleBitBinaryInput) => Some(event.asInstanceOf[TypedEvent[DoubleBitBinaryInput]])
      case _ => None
    }
  }

  def getUnhandledDoubleBitBinaryInputEvents(batch: BatchSpecifier): Seq[TypedEvent[DoubleBitBinaryInput]] = {
    getAllDoubleBitBinaryInputEvents(batch).filter(e => !e.isHandled)
  }

  def popDoubleBitBinaryEvent(batch: BatchSpecifier): Option[TypedEvent[DoubleBitBinaryInput]] = {
    val head = getUnhandledDoubleBitBinaryInputEvents(batch).headOption
    head match {
      case Some(event) => event.handle()
      case None =>
    }
    head
  }

  def getAllCounters: Seq[TypedEvent[Counter]] = {
    this.counters.map(e => {
      new TypedEvent(e._1, 0, EventClass.All, e._2)
    }).toSeq
  }

  def getRecordedCounters: Seq[TypedEvent[Counter]] = {
    this.recordedCounterValues.map(e => {
      new TypedEvent(e._1, 0, EventClass.All, e._2)
    }).toSeq
  }

  def getAllCounterEvents(batch: BatchSpecifier): Seq[TypedEvent[Counter]] = {
    getAllEvents(batch).flatMap {
      case event @ TypedEvent(_: Counter) => Some(event.asInstanceOf[TypedEvent[Counter]])
      case _ => None
    }
  }

  def getUnhandledCounterEvents(batch: BatchSpecifier): Seq[TypedEvent[Counter]] = {
    getAllCounterEvents(batch).filter(e => !e.isHandled)
  }

  def popCounterEvent(batch: BatchSpecifier): Option[TypedEvent[Counter]] = {
    val head = getUnhandledCounterEvents(batch).headOption
    head match {
      case Some(event) => event.handle()
      case None =>
    }
    head
  }

  def getAllAnalogInputs: Seq[TypedEvent[AnalogInput]] = {
    this.analogInputs.map(e => {
      new TypedEvent(e._1, 0, EventClass.All, e._2)
    }).toSeq
  }

  def getAllAnalogInputEvents(batch: BatchSpecifier): Seq[TypedEvent[AnalogInput]] = {
    getAllEvents(batch).flatMap {
      case event @ TypedEvent(_: AnalogInput) => Some(event.asInstanceOf[TypedEvent[AnalogInput]])
      case _ => None
    }
  }

  def getUnhandledAnalogInputEvents(batch: BatchSpecifier): Seq[TypedEvent[AnalogInput]] = {
    getAllAnalogInputEvents(batch).filter(e => !e.isHandled)
  }

  def popAnalogInputEvent(batch: BatchSpecifier): Option[TypedEvent[AnalogInput]] = {
    val head = getUnhandledAnalogInputEvents(batch).headOption
    head match {
      case Some(event) => event.handle()
      case None =>
    }
    head
  }
}
