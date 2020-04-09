package com.automatak.dnp4sopendnp3

import com.automatak.dnp3.{LogEntry, LogHandler}

class CustomLogHandler extends LogHandler {
  override def log(entry: LogEntry): Unit = {
    // We don't keep these logs, we only use the test harness ones
  }
}
