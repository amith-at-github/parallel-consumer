package io.confluent.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.parallelconsumer.BatchTestMethods;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.RateLimiter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;

@Slf4j
public class ReactorBatchTest extends ReactorUnitTestBase {

    BatchTestMethods<Mono<String>> batchTestMethods;

    @BeforeEach
    void setup() {
        batchTestMethods = new BatchTestMethods<>() {

            @Override
            protected KafkaTestUtils getKtu() {
                return ktu;
            }

            @SneakyThrows
            @Override
            protected Mono<String> pollStep(List<ConsumerRecord<String, String>> recordList) {
                return Mono.just(msg("Saw batch or records: {}", toOffsets(recordList)))
                        .delayElement(Duration.ofMillis(30));
            }

            @Override
            protected void averageBatchSizePoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger) {
                reactorPC.reactBatch(recordList -> {
                    return pollInner(numBatches, numRecords, statusLogger, recordList);
                });
            }

            @Override
            public void setupParallelConsumer(int targetBatchSize, int maxConcurrency, ParallelConsumerOptions.ProcessingOrder ordering) {
                //
                ParallelConsumerOptions<Object, Object> options = ParallelConsumerOptions.builder()
                        .batchSize(targetBatchSize)
                        .ordering(ordering)
                        .maxConcurrency(maxConcurrency)
//                .processorDelayMs(processorDelayMs)
//                .initialDynamicLoadFactor(initialDynamicLoadFactor)
                        .build();

                reactorPC.setTimeBetweenCommits(Duration.ofSeconds(5));

                setupParallelConsumerInstance(options);
            }

            @Override
            public void batchPoll(List<List<ConsumerRecord<String, String>>> received) {
                reactorPC.reactBatch(recordList -> {
                    log.debug("Batch of messages: {}", toOffsets(recordList));
                    received.add(recordList);
                    return Mono.just(msg("Saw batch or records: {}", toOffsets(recordList)));
                });
            }
        };
    }

    @Test
    void averageBatchSizeTest() {
        batchTestMethods.averageBatchSizeTestMethod(10000);
    }

    @ParameterizedTest
    @EnumSource
    void batch(ParallelConsumerOptions.ProcessingOrder order) {
        batchTestMethods.batchTestMethod(order);
    }

}
