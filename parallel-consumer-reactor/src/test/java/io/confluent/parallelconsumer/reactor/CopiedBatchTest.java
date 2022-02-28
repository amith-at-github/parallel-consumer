package io.confluent.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * Basic tests for batch processing functionality
 */
//tood delete
@Disabled
@Slf4j
public class CopiedBatchTest extends ReactorUnitTestBase {

    @Test
    void averageBatchSizeTest() {
        // test settings
        int numRecs = 50000;
        final int targetBatchSize = 20;
        int maxConcurrency = 8;
        int processorDelayMs = 30;
        int initialDynamicLoadFactor = targetBatchSize * 100;

        //
        ParallelConsumerOptions<Object, Object> options = ParallelConsumerOptions.builder()
                .batchSize(targetBatchSize)
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .maxConcurrency(maxConcurrency)
//                .processorDelayMs(processorDelayMs)
//                .initialDynamicLoadFactor(initialDynamicLoadFactor)
                .build();

        super.setupParallelConsumerInstance(options);

        //
        ktu.sendRecords(numRecs);

        //
        var numBatches = new AtomicInteger(0);
        var numRecords = new AtomicInteger(0);
        long start = System.currentTimeMillis();
        RateLimiter statusLogger = new RateLimiter(1);

        // poll
        super.reactorPC.reactBatch(recordList -> {
            int size = recordList.size();

            statusLogger.performIfNotLimited(() -> {
                try {
                    log.debug(
                            "Processed {} records in {} batches with average size {}",
                            numRecords.get(),
                            numBatches.get(),
                            calcAverage(numRecords, numBatches)
                    );
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            });

            try {
                log.trace("Batch size {}", size);
                return Mono.just(msg("Saw batch or records: {}", toOffsets(recordList)))
                        .delayElement(Duration.ofMillis(30));
            } finally {
                numBatches.getAndIncrement();
                numRecords.addAndGet(size);
            }
        });

        //
        waitAtMost(ofSeconds(200)).alias("expected number of records")
                .untilAsserted(() -> {
                    assertThat(numRecords.get()).isEqualTo(numRecs);
                });

        //
        var duration = System.currentTimeMillis() - start;
        double averageBatchSize = calcAverage(numRecords, numBatches);
        log.debug("Processed {} records in {} ms. Average batch size was: {}. {} records per second.", numRecs, duration, averageBatchSize, numRecs / (duration / 1000.0));

        //
        double targetMetThreshold = 0.9;
        double acceptableAttainedBatchSize = targetBatchSize * targetMetThreshold;
        assertThat(averageBatchSize).isGreaterThan(acceptableAttainedBatchSize);
    }

    private double calcAverage(AtomicInteger numRecords, AtomicInteger numBatches) {
        return numRecords.get() / (0.0 + numBatches.get());
    }

    @ParameterizedTest
    @EnumSource
    void batch(ParallelConsumerOptions.ProcessingOrder order) {
        int numRecs = 5;
        int batchSize = 2;
        super.setupParallelConsumerInstance(ParallelConsumerOptions.builder()
                .batchSize(batchSize)
                .ordering(order)
                .build());
        var recs = ktu.sendRecords(numRecs);
        List<List<ConsumerRecord<String, String>>> received = new ArrayList<>();

        //
        reactorPC.reactBatch(x -> {
            log.debug("Batch of messages: {}", toOffsets(x));
            received.add(x);
            return Mono.just(msg("Batch of messages: {}", toOffsets(x)));
        });

        //
        int expectedNumOfBatches = (order == PARTITION) ?
                numRecs : // partition ordering restricts the batch sizes to a single element as all records are in a single partition
                (int) Math.ceil(numRecs / (double) batchSize);

        waitAtMost(ofSeconds(1)).alias("expected number of batches")
                .untilAsserted(() -> {
                    assertThat(received).hasSize(expectedNumOfBatches);
                });

        assertThat(received)
                .as("batch size")
                .allSatisfy(x -> assertThat(x).hasSizeLessThanOrEqualTo(batchSize))
                .as("all messages processed")
                .flatExtracting(x -> x).hasSameElementsAs(recs);
    }

    private List<Long> toOffsets(final List<ConsumerRecord<String, String>> x) {
        return x.stream().map(ConsumerRecord::offset).collect(Collectors.toList());
    }

}