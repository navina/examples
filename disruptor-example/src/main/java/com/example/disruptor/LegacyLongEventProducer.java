package com.example.disruptor;

import com.lmax.disruptor.RingBuffer;
import java.nio.ByteBuffer;


public class LegacyLongEventProducer {
  private final RingBuffer<LongEvent> _ringBuffer;

  public LegacyLongEventProducer(RingBuffer<LongEvent> ringBuffer) {
    this._ringBuffer = ringBuffer;
  }

  public void onData(ByteBuffer bb) {
    long sequence = _ringBuffer.next(); // Grab the next sequence
    try {
      LongEvent event = _ringBuffer.get(sequence);  // Get the entry in the Disruptor for the sequence

      event.set(bb.getLong(0)); // Fill with data
    } finally {
      _ringBuffer.publish(sequence);
    }
  }
}
