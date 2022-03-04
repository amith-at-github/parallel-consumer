package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.RateLimiter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Basic tests for batch processing functionality
 */
@Timeout(value = 1, unit = MINUTES)
@Slf4j
public class BatchTest extends ParallelEoSStreamProcessorTestBase implements BatchTestBase {

    BatchTestMethods<Void> batchTestMethods;

    @BeforeEach
    void setup() {
        batchTestMethods = new BatchTestMethods<>(this) {

            @Override
            protected KafkaTestUtils getKtu() {
                return ktu;
            }

            @SneakyThrows
            @Override
            protected Void averageBatchSizeTestPollStep(List<ConsumerRecord<String, String>> recordList) {
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
                return null;
            }

            @Override
            protected void averageBatchSizeTestPoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter
                    statusLogger) {
                parallelConsumer.pollBatch(recordList -> {
                    averageBatchSizeTestPollInner(numBatches, numRecords, statusLogger, recordList);
                });
            }

            @Override
            protected AbstractParallelEoSStreamProcessor getPC() {
                return parallelConsumer;
            }

            @Override
            public void simpleBatchTestPoll(List<List<ConsumerRecord<String, String>>> received) {
                parallelConsumer.pollBatch(pollBatch -> {
                    log.debug("Batch of messages: {}", toOffsets(pollBatch));
                    received.add(pollBatch);
                });
            }

            @Override
            protected void batchFailPoll(List<List<ConsumerRecord<String, String>>> received) {
                parallelConsumer.pollBatch(pollBatch -> {
                    batchFailPollInner(pollBatch);
                    received.add(pollBatch);
                });
            }
        };
    }

    @Test
    public void averageBatchSizeTest() {
        batchTestMethods.averageBatchSizeTest(50000);
    }

    @ParameterizedTest
    @EnumSource
    @Override
    public void simpleBatchTest(ParallelConsumerOptions.ProcessingOrder order) {
        batchTestMethods.simpleBatchTest(order);
    }

    @ParameterizedTest
    @EnumSource
    @Override
    public void batchFailureTest(ParallelConsumerOptions.ProcessingOrder order) {
        batchTestMethods.batchFailureTest(order);
    }

}