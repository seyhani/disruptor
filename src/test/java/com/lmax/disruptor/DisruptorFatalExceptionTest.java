package com.lmax.disruptor;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.support.TestEvent;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DisruptorFatalExceptionTest
{
    @Test
    public void shouldStopProcessingOnFatalException() throws Exception
    {
        Disruptor<TestEvent> disruptor = new Disruptor<>(
                TestEvent::new, 16, DaemonThreadFactory.INSTANCE, ProducerType.SINGLE, new BlockingWaitStrategy());
        AtomicBoolean handled = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);

        disruptor.handleEventsWith((event, sequence, endOfBatch) ->
        {
            started.countDown();
            throw new IllegalStateException("fatal");
        });
        disruptor.setDefaultExceptionHandler(new FatalExceptionHandler());

        disruptor.start();
        disruptor.publishEvent((event, sequence) -> { });
        assertTrue(started.await(1, TimeUnit.SECONDS));

        // allow handler thread to process and fail
        for (int i = 0; i < 10 && disruptor.isRunning(); i++)
        {
            TimeUnit.MILLISECONDS.sleep(50);
        }

        assertFalse(disruptor.isRunning(), "Disruptor should not be running after fatal exception");
        disruptor.halt();
    }
}
