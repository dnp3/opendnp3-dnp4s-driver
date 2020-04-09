package com.automatak.dnp4sopendnp3

import com.automatak.dnp3.ChannelListener
import com.automatak.dnp3.enums.ChannelState

class CustomChannelListener extends ChannelListener {
  override def onStateChange(state: ChannelState): Unit = {
    // We don't need this information
  }
}
