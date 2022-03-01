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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofSeconds;

/**
 * Basic tests for batch processing functionality
 */
@Slf4j
public class BatchTest extends ParallelEoSStreamProcessorTestBase {

    BatchTestMethods<Void> batchTestMethods;

    @BeforeEach
    void setup() {
        batchTestMethods = new BatchTestMethods<>() {

            @Override
            protected KafkaTestUtils getKtu() {
                return ktu;
            }

            @SneakyThrows
            @Override
            protected Void pollStep(List<ConsumerRecord<String, String>> recordList) {
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
                return null;
            }

            @Override
            protected void averageBatchSizePoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter
                    statusLogger) {
                parallelConsumer.pollBatch(recordList -> {
                    pollInner(numBatches, numRecords, statusLogger, recordList);
                });
            }

            @Override
            protected void setupParallelConsumer(int targetBatchSize, int maxConcurrency, ParallelConsumerOptions.ProcessingOrder ordering) {
                //
                setupParallelConsumerInstance(ParallelConsumerOptions.builder()
                        .batchSize(targetBatchSize)
                        .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                        .maxConcurrency(maxConcurrency)
//                .processorDelayMs(processorDelayMs)
//                .initialDynamicLoadFactor(initialDynamicLoadFactor)
                        .build());

                //
                parallelConsumer.setTimeBetweenCommits(ofSeconds(5));
            }

            @Override
            protected AbstractParallelEoSStreamProcessor getPC() {
                return parallelConsumer;
            }

            @Override
            public void batchPoll(List<List<ConsumerRecord<String, String>>> received) {
                parallelConsumer.pollBatch(x -> {
                    log.debug("Batch of messages: {}", toOffsets(x));
                    received.add(x);
                });
            }
        };
    }


    @Test
    void averageBatchSizeTest() {
        batchTestMethods.averageBatchSizeTestMethod(50000);
    }

    @ParameterizedTest
    @EnumSource
    void batch(ParallelConsumerOptions.ProcessingOrder order) {
        batchTestMethods.batchTestMethod(order);
    }

}