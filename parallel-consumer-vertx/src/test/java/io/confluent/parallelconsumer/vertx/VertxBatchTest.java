package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.parallelconsumer.BatchTestMethods;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.RateLimiter;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;

@Slf4j
@ExtendWith(VertxExtension.class)
public class VertxBatchTest extends VertxBaseUnitTest {

    private Vertx vertx;
    private VertxTestContext tc;

    BatchTestMethods<Future<String>> batchTestMethods;

    @BeforeEach
    void setup() {
        batchTestMethods = new BatchTestMethods<>(this) {

            @Override
            protected KafkaTestUtils getKtu() {
                return ktu;
            }

            @SneakyThrows
            @Override
            protected Future<String> pollStep(List<ConsumerRecord<String, String>> recordList) {
                int delayInMs = 30;

                Promise<String> promise = Promise.promise();

                vertx.setTimer(delayInMs, event -> {
                    String msg = msg("Saw batch or records: {}", toOffsets(recordList));
                    log.debug(msg);
                    promise.complete(msg);
                });

                return promise.future();
            }

            @Override
            protected void averageBatchSizePoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger) {
                vertxAsync.batchVertxFuture(recordList -> {
                    return pollInner(numBatches, numRecords, statusLogger, recordList);
                });
            }

            @Override
            protected AbstractParallelEoSStreamProcessor getPC() {
                return vertxAsync;
            }

            @Override
            public void batchPoll(List<List<ConsumerRecord<String, String>>> received) {
                vertxAsync.batchVertxFuture(recordList -> {
                    return vertx.executeBlocking(event -> {
                        log.debug("Saw batch or records: {}", toOffsets(recordList));
                        received.add(recordList);

                        event.complete(msg("Saw batch or records: {}", toOffsets(recordList)));
                    });
                });
            }
        };
    }

    @Test
    void averageBatchSizeTest(Vertx vertx, VertxTestContext tc) {
        this.vertx = vertx;
        this.tc = tc;
        batchTestMethods.averageBatchSizeTestMethod(10000);
        tc.completeNow();
    }

    @ParameterizedTest
    @EnumSource
    void batch(ParallelConsumerOptions.ProcessingOrder order, Vertx vertx, VertxTestContext tc) {
        this.vertx = vertx;
        this.tc = tc;
        batchTestMethods.batchTestMethod(order);
        tc.completeNow();
    }
}
