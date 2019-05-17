package com.example.disruptor;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import java.nio.ByteBuffer;


public class LongEventMain {

  public static void main(String[] args) {
    LongEventFactory eventFactory = new LongEventFactory();

    int bufferSize = 1024;  // specify size of the ring buffer

    Disruptor<LongEvent> disruptor =
        new Disruptor<>(eventFactory, bufferSize, DaemonThreadFactory.INSTANCE);

    // Connect the handler to the ring buffer
    disruptor.handleEventsWith(new LongEventHandler()); // Can use Lambda instead of a custom class for EventHandler

    // Start the disruptor, starts all threads running
    disruptor.start();

    // Get RingBuffer from the disruptor to be used for publishing
    RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();

//    LegacyLongEventProducer legacyProducer = new LegacyLongEventProducer(ringBuffer);
    LongEventProducer producer = new LongEventProducer(ringBuffer);

    ByteBuffer bb = ByteBuffer.allocate(8);
    for (long l = 0; true; l++) {
      bb.putLong(0, l);
//      legacyProducer.onData(bb);
      producer.onData(bb);  // Can use PublishEvent and provide a function that serves as the event producer, instead
      // of the custom class; Reduces boilerplate required for the code.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
