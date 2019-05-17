package com.example.disruptor;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import java.nio.ByteBuffer;

/**
 * Advantage of using a translator:
 * 1. translator code can be pulled into a separate class and independently tested
 * 2. Transaction write implementation is hidden from the user of the API. So, if the producer claims a spot and doesn't
 * fill it for whatever reason (maybe error), it will stall the consumers downstream. This can be safely avoided if
 * implemented as translator. Especially useful in the multi-producer case.
 *
 */
public class LongEventProducer {
  private final RingBuffer<LongEvent> _ringBuffer;

  public LongEventProducer(RingBuffer<LongEvent> ringBuffer) {
    _ringBuffer = ringBuffer;
  }

  private static final EventTranslatorOneArg<LongEvent, ByteBuffer> TRANSLATOR =
      new EventTranslatorOneArg<LongEvent, ByteBuffer>() {
        @Override
        public void translateTo(LongEvent event, long sequence, ByteBuffer arg0) {
          event.set(arg0.getLong(0));
        }
      };

  public void onData(ByteBuffer bb) {
    _ringBuffer.publishEvent(TRANSLATOR, bb);
  }
}
