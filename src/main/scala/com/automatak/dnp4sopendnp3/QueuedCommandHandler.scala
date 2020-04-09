package com.automatak.dnp4sopendnp3

import java.util.concurrent.ConcurrentLinkedDeque

import com.automatak.dnp3.enums.{AnalogQuality, CommandStatus, ControlCode, OperateType}
import com.automatak.dnp3.{AnalogOutputDouble64, AnalogOutputFloat32, AnalogOutputInt16, AnalogOutputInt32, AnalogOutputStatus, CommandHandler, ControlRelayOutputBlock, Database, Flags, Outstation, OutstationChangeSet}

class QueuedCommandHandler(val app: CustomOutstationApplication, val binaryOutputsDisabled: Boolean, val analogOutputsDisabled: Boolean) extends CommandHandler {
  val waitTime = 1000

  private val binaryOutputCommands = new ConcurrentLinkedDeque[Integer]()
  private val analogOutputCommands = new ConcurrentLinkedDeque[Integer]()

  private var outstation: Outstation = null

  def setOutstation(outstation: Outstation) = {
    this.outstation = outstation
  }

  override def begin(): Unit = {}

  override def end(): Unit = {}

  override def select(command: ControlRelayOutputBlock, index: Int): CommandStatus = {
    checkBinaryOutputCommand(command, index, false)
  }

  override def select(command: AnalogOutputInt32, index: Int): CommandStatus = {
    checkAnalogOutputCommand(command.value.toDouble, index, false)
  }

  override def select(command: AnalogOutputInt16, index: Int): CommandStatus = {
    checkAnalogOutputCommand(command.value.toDouble, index, false)
  }

  override def select(command: AnalogOutputFloat32, index: Int): CommandStatus = {
    checkAnalogOutputCommand(command.value.toDouble, index, false)
  }

  override def select(command: AnalogOutputDouble64, index: Int): CommandStatus = {
    checkAnalogOutputCommand(command.value, index, false)
  }

  override def operate(command: ControlRelayOutputBlock, index: Int, db: Database, opType: OperateType): CommandStatus = {
    checkBinaryOutputCommand(command, index, true)
  }

  override def operate(command: AnalogOutputInt32, index: Int, db: Database, opType: OperateType): CommandStatus = {
    checkAnalogOutputCommand(command.value.toDouble, index, true)
  }

  override def operate(command: AnalogOutputInt16, index: Int, db: Database, opType: OperateType): CommandStatus = {
    checkAnalogOutputCommand(command.value.toDouble, index, true)
  }

  override def operate(command: AnalogOutputFloat32, index: Int, db: Database, opType: OperateType): CommandStatus = {
    checkAnalogOutputCommand(command.value.toDouble, index, true)
  }

  override def operate(command: AnalogOutputDouble64, index: Int, db: Database, opType: OperateType): CommandStatus = {
    checkAnalogOutputCommand(command.value, index, true)
  }

  def checkCrob(index: Int): Unit = {
    val startTime = System.currentTimeMillis()

    while(System.currentTimeMillis() - startTime < waitTime) {
      val value = binaryOutputCommands.pollFirst()
      if(value != null) {
        if(value == index) return
        else throw new Exception(s"Unexpected operate Binary Output $value ($index expected)")
      }
      Thread.sleep(100)
    }

    throw new Exception(s"Binary Output $index never operated")
  }

  def checkNoCrob(): Unit = {
    val startTime = System.currentTimeMillis()

    while(System.currentTimeMillis() - startTime < waitTime) {
      val value = binaryOutputCommands.pollFirst()
      if(value != null) {
        throw new Exception(s"Unexpected operate on Binary Output $value (none expected)")
      }
      Thread.sleep(100)
    }
  }

  def checkAnalog(index: Int): Unit = {
    val startTime = System.currentTimeMillis()

    while(System.currentTimeMillis() - startTime < waitTime) {
      val value = analogOutputCommands.pollFirst()
      if(value != null) {
        if(value == index) return
        else throw new Exception(s"Unexpected operate Analog Output $value ($index expected)")
      }
      Thread.sleep(100)
    }

    throw new Exception(s"Analog Output $index never operated")
  }

  def checkNoAnalog(): Unit = {
    val startTime = System.currentTimeMillis()

    while(System.currentTimeMillis() - startTime < waitTime) {
      val value = analogOutputCommands.pollFirst()
      if(value != null) {
        throw new Exception(s"Unexpected operate on Analog Output $value (none expected)")
      }
      Thread.sleep(100)
    }
  }

  private def checkBinaryOutputCommand(crob: ControlRelayOutputBlock, index: Int, operate: Boolean) : CommandStatus = {
    if (binaryOutputsDisabled) return CommandStatus.NOT_SUPPORTED

    val result = index match {
      case 0 =>
        if(crob.function == ControlCode.LATCH_ON &&
           crob.count == 1 &&
           crob.onTimeMs == 100 &&
           crob.offTimeMs == 200) {
          CommandStatus.SUCCESS
        }
        else CommandStatus.FORMAT_ERROR
      case 1 =>
        if(crob.function == ControlCode.TRIP_PULSE_ON &&
          crob.count == 5 &&
          crob.onTimeMs == 300 &&
          crob.offTimeMs == 400) {
          CommandStatus.SUCCESS
        }
        else CommandStatus.FORMAT_ERROR
      case 2 =>
        if((crob.function == ControlCode.LATCH_ON || crob.function == ControlCode.LATCH_OFF) &&
          crob.count == 10 &&
          crob.onTimeMs == 500 &&
          crob.offTimeMs == 600) {
          CommandStatus.SUCCESS
        }
        else CommandStatus.FORMAT_ERROR
      case 3 =>
        if((crob.function == ControlCode.TRIP_PULSE_ON || crob.function == ControlCode.CLOSE_PULSE_ON) &&
          crob.count == 15 &&
          crob.onTimeMs == 700 &&
          crob.offTimeMs == 800) {
          CommandStatus.SUCCESS
        }
        else CommandStatus.FORMAT_ERROR
      case 4 =>
        if((crob.function == ControlCode.LATCH_ON || crob.function == ControlCode.LATCH_OFF || crob.function == ControlCode.TRIP_PULSE_ON || crob.function == ControlCode.CLOSE_PULSE_ON) &&
          crob.count == 20 &&
          crob.onTimeMs == 900 &&
          crob.offTimeMs == 1000) {
          CommandStatus.SUCCESS
        }
        else CommandStatus.FORMAT_ERROR
      case i if i >= 10 && i <= 19 =>
        if(crob.function == ControlCode.LATCH_ON &&
          crob.count == 1 &&
          crob.onTimeMs == 100 &&
          crob.offTimeMs == 200) {
          CommandStatus.SUCCESS
        }
        else CommandStatus.FORMAT_ERROR
      case _ => CommandStatus.NOT_SUPPORTED
    }

    if(operate && result == CommandStatus.SUCCESS) {
      binaryOutputCommands.add(index)
    }

    result
  }

  private def checkAnalogOutputCommand(value: Double, index: Int, operate: Boolean) : CommandStatus = {
    if (analogOutputsDisabled) return CommandStatus.NOT_SUPPORTED

    val result = index match {
      case 0 =>
        if(value == 10.0) CommandStatus.SUCCESS else CommandStatus.FORMAT_ERROR
      case 1 => if(value == 20.0) CommandStatus.SUCCESS else CommandStatus.FORMAT_ERROR
      case i if i >= 10 && i <= 19 => if(value == 10.0) CommandStatus.SUCCESS else CommandStatus.FORMAT_ERROR
      case _ => CommandStatus.NOT_SUPPORTED
    }

    if(operate && result == CommandStatus.SUCCESS) {
      analogOutputCommands.add(index)
      val changeSet = new OutstationChangeSet()
      val flags = new Flags();
      flags.set(AnalogQuality.ONLINE);
      changeSet.update(new AnalogOutputStatus(value, flags, app.now()), index)
      outstation.apply(changeSet)
    }

    result
  }
}
