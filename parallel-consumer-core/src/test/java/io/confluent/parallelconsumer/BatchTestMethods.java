package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.RateLimiter;
import io.confluent.parallelconsumer.truth.LongPollingMockConsumerSubject;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

@Slf4j
@RequiredArgsConstructor
public abstract class BatchTestMethods<POLL_RETURN> {

    //todo rename
    public void averageBatchSizeTestMethod(int numRecsExpected) {
        // test settings
        final int targetBatchSize = 20;
        int maxConcurrency = 8;
        int processorDelayMs = 30;
        int initialDynamicLoadFactor = targetBatchSize * 100;

        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, numRecsExpected);

        setupParallelConsumer(targetBatchSize, maxConcurrency, UNORDERED);

        //
        getKtu().sendRecords(numRecsExpected);

        //
        var numBatches = new AtomicInteger(0);
        var numRecordsProcessed = new AtomicInteger(0);
        long start = System.currentTimeMillis();
        RateLimiter statusLogger = new RateLimiter(1);

        averageBatchSizePoll(numBatches, numRecordsProcessed, statusLogger);

        //
        waitAtMost(ofSeconds(200)).alias("expected number of records")
                .untilAsserted(() -> {
                    bar.stepTo(numRecordsProcessed.get());
                    assertThat(numRecordsProcessed.get()).isEqualTo(numRecsExpected);
                });
        bar.close();

        //
        var duration = System.currentTimeMillis() - start;
        double averageBatchSize = calcAverage(numRecordsProcessed, numBatches);
        log.debug("Processed {} records in {} ms. Average batch size was: {}. {} records per second.", numRecsExpected, duration, averageBatchSize, numRecsExpected / (duration / 1000.0));

        //
        double targetMetThreshold = 0.9;
        double acceptableAttainedBatchSize = targetBatchSize * targetMetThreshold;
        assertThat(averageBatchSize).isGreaterThan(acceptableAttainedBatchSize);
        assertThat(getPC().isClosedOrFailed()).isFalse();

        LongPollingMockConsumerSubject.assertThat(getKtu().getConsumerSpy())
                .hasCommittedToAnyPartition()
                .atLeastOffset(numRecsExpected);
    }

    protected abstract KafkaTestUtils getKtu();

    //todo rename
    protected POLL_RETURN pollInner(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger, List<ConsumerRecord<String, String>> recordList) {
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
            return pollStep(recordList);
        } finally {
            numBatches.getAndIncrement();
            numRecords.addAndGet(size);
        }
    }

    //todo rename
    protected abstract POLL_RETURN pollStep(List<ConsumerRecord<String, String>> recordList);

    /**
     * Must call {@link #pollInner}
     */
    protected abstract void averageBatchSizePoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger);

    private double calcAverage(AtomicInteger numRecords, AtomicInteger numBatches) {
        return numRecords.get() / (0.0 + numBatches.get());
    }

    protected abstract void setupParallelConsumer(int targetBatchSize, int maxConcurrency, ParallelConsumerOptions.ProcessingOrder ordering);

    //todo rename
    @SneakyThrows
    public void batchTestMethod(ParallelConsumerOptions.ProcessingOrder order) {
        int batchSize = 2;
        int numRecsExpected = 5;

        setupParallelConsumer(batchSize, ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY, order);

        var recs = getKtu().sendRecords(numRecsExpected);
        List<List<ConsumerRecord<String, String>>> received = new ArrayList<>();

        //
        batchPoll(received);

        //
        int expectedNumOfBatches = (order == PARTITION) ?
                numRecsExpected : // partition ordering restricts the batch sizes to a single element as all records are in a single partition
                (int) Math.ceil(numRecsExpected / (double) batchSize);

        waitAtMost(ofSeconds(5)).alias("expected number of batches")
                .failFast(() -> getPC().isClosedOrFailed())
                .untilAsserted(() -> {
                    assertThat(received).hasSize(expectedNumOfBatches);
                    assertThat(received.stream().mapToLong(Collection::size).sum()).isEqualTo(numRecsExpected);
                });

        assertThat(received)
                .as("batch size")
                .allSatisfy(x -> assertThat(x).hasSizeLessThanOrEqualTo(batchSize))
                .as("all messages processed")
                .flatExtracting(x -> x).hasSameElementsAs(recs);
        assertThat(getPC().isClosedOrFailed()).isFalse();

        Thread.sleep(10000);

        // todo fix assertion of actual committed offsets
        LongPollingMockConsumerSubject.assertThat(getKtu().getConsumerSpy())
                .hasCommittedToAnyPartition()
                .atLeastOffset(numRecsExpected);
    }

    protected abstract AbstractParallelEoSStreamProcessor getPC();

    //todo rename
    public abstract void batchPoll(List<List<ConsumerRecord<String, String>>> received);

    public List<Long> toOffsets(final List<ConsumerRecord<String, String>> x) {
        return x.stream().map(ConsumerRecord::offset).collect(Collectors.toList());
    }

}
