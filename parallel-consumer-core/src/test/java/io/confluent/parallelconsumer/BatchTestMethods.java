package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.RateLimiter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * Batch test which can be used in the different modules. The need for this is because the batch methods in each module
 * all have different signatures, and return types.
 */
@Slf4j
@RequiredArgsConstructor
public abstract class BatchTestMethods<POLL_RETURN> {

    public static final long FAILURE_TARGET = 5L;
    private final ParallelEoSStreamProcessorTestBase baseTest;

    protected abstract KafkaTestUtils getKtu();


    protected void setupParallelConsumer(int targetBatchSize, int maxConcurrency, ParallelConsumerOptions.ProcessingOrder ordering) {
        //
        ParallelConsumerOptions<Object, Object> options = ParallelConsumerOptions.builder()
                .batchSize(targetBatchSize)
                .ordering(ordering)
                .maxConcurrency(maxConcurrency)
                .build();
        baseTest.setupParallelConsumerInstance(options);

        //
        baseTest.parentParallelConsumer.setTimeBetweenCommits(ofSeconds(5));
    }

    protected abstract AbstractParallelEoSStreamProcessor getPC();

    public List<Long> toOffsets(final List<ConsumerRecord<String, String>> x) {
        return x.stream().map(ConsumerRecord::offset).collect(Collectors.toList());
    }

    public void averageBatchSizeTest(int numRecsExpected) {
        final int targetBatchSize = 20;
        int maxConcurrency = 8;

        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, numRecsExpected);

        setupParallelConsumer(targetBatchSize, maxConcurrency, UNORDERED);

        //
        getKtu().sendRecords(numRecsExpected);

        //
        var numBatches = new AtomicInteger(0);
        var numRecordsProcessed = new AtomicInteger(0);
        long start = System.currentTimeMillis();
        RateLimiter statusLogger = new RateLimiter(1);

        averageBatchSizeTestPoll(numBatches, numRecordsProcessed, statusLogger);

        //
        waitAtMost(defaultTimeout).alias("expected number of records")
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
        double targetMetThreshold = 3. / 4.;
        double acceptableAttainedBatchSize = targetBatchSize * targetMetThreshold;
        assertThat(averageBatchSize).isGreaterThan(acceptableAttainedBatchSize);

        baseTest.parentParallelConsumer.requestCommitAsap();
        baseTest.awaitForCommit(numRecsExpected);
    }

    /**
     * Must call {@link #averageBatchSizeTestPollInner}
     */
    protected abstract void averageBatchSizeTestPoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger);

    protected POLL_RETURN averageBatchSizeTestPollInner(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger, List<ConsumerRecord<String, String>> recordList) {
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
            return averageBatchSizeTestPollStep(recordList);
        } finally {
            numBatches.getAndIncrement();
            numRecords.addAndGet(size);
        }
    }

    protected abstract POLL_RETURN averageBatchSizeTestPollStep(List<ConsumerRecord<String, String>> recordList);

    private double calcAverage(AtomicInteger numRecords, AtomicInteger numBatches) {
        return numRecords.get() / (0.0 + numBatches.get());
    }


    @SneakyThrows
    public void simpleBatchTest(ParallelConsumerOptions.ProcessingOrder order) {
        int batchSizeSetting = 2;
        int numRecsExpected = 5;

        getPC().setTimeBetweenCommits(ofSeconds(1));

        setupParallelConsumer(batchSizeSetting, ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY, order);

        var recs = getKtu().sendRecords(numRecsExpected);
        List<List<ConsumerRecord<String, String>>> batchesReceived = new CopyOnWriteArrayList<>();

        //
        simpleBatchTestPoll(batchesReceived);

        //
        int expectedNumOfBatches = (order == PARTITION) ?
                numRecsExpected : // partition ordering restricts the batch sizes to a single element as all records are in a single partition
                (int) Math.ceil(numRecsExpected / (double) batchSizeSetting);

        waitAtMost(defaultTimeout).alias("expected number of batches")
                .failFast(() -> getPC().isClosedOrFailed())
                .untilAsserted(() -> {
                    assertThat(batchesReceived).hasSize(expectedNumOfBatches);
                });

        assertThat(batchesReceived)
                .as("batch size")
                .allSatisfy(receivedBatchEntry -> assertThat(receivedBatchEntry).hasSizeLessThanOrEqualTo(batchSizeSetting))
                .as("all messages processed")
                .flatExtracting(x -> x).hasSameElementsAs(recs);

        assertThat(getPC().isClosedOrFailed()).isFalse();

//        await().atMost(defaultTimeout)
//                .untilAsserted(() -> {
//                    long numberOfEntriesInPartitionQueues = getPC().getWm().getNumberOfEntriesInPartitionQueues();
//                    ShardManager sm = getPC().getWm().getSm();
//                    PartitionMonitor pm = getPC().getWm().getPm();
//                    long numberOfWorkQueuedInShardsAwaitingSelection = sm.getNumberOfWorkQueuedInShardsAwaitingSelection();
//                    Set assignment = getKtu().getConsumerSpy().assignment();
//                    TopicPartition o = (TopicPartition) assignment.stream().findFirst().get();
//                    PartitionState partitionState = pm.getPartitionState(o);
//                    Set incompleteOffsets = partitionState.getIncompleteOffsets();
//
//                    assertThat(incompleteOffsets).isEmpty();
//                });


        baseTest.awaitForCommit(numRecsExpected);
//        getPC().requestCommitAsap();
        getPC().closeDrainFirst();
    }

    public abstract void simpleBatchTestPoll(List<List<ConsumerRecord<String, String>>> received);

    @SneakyThrows
    public void batchFailureTest(ParallelConsumerOptions.ProcessingOrder order) {
        int batchSize = 5;
        int expectedNumOfMessages = 20;

        setupParallelConsumer(batchSize, ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY, order);

        var recs = getKtu().sendRecords(expectedNumOfMessages);
        List<List<ConsumerRecord<String, String>>> receivedBatches = Collections.synchronizedList(new ArrayList<>());

        //
        batchFailPoll(receivedBatches);

        //
        int expectedNumOfBatches = (int) Math.ceil(expectedNumOfMessages / (double) batchSize);

        baseTest.awaitForCommit(expectedNumOfMessages);

        // due to the failure, might get one extra batch
        assertThat(receivedBatches).hasSizeGreaterThanOrEqualTo(expectedNumOfBatches);

        assertThat(receivedBatches)
                .as("batch size")
                .allSatisfy(receivedBatch ->
                        assertThat(receivedBatch).hasSizeLessThanOrEqualTo(batchSize))
                .as("all messages processed")
                .flatExtracting(x -> x).hasSameElementsAs(recs);

        //
        assertThat(getPC().isClosedOrFailed()).isFalse();
    }

    /**
     * Must call {@link #batchFailPollInner(List)}
     */
    protected abstract void batchFailPoll(List<List<ConsumerRecord<String, String>>> received);

    protected POLL_RETURN batchFailPollInner(List<ConsumerRecord<String, String>> received) {
        List<Long> offsets = received.stream().map(ConsumerRecord::offset).collect(Collectors.toList());
        log.debug("Got batch {}", offsets);

        boolean contains = offsets.contains(FAILURE_TARGET);
        if (contains) {
            var target = received.stream().filter(x -> x.offset() == FAILURE_TARGET).findFirst().get();
            var targetWc = getPC().getWm().getWorkContainerFor(target);
            int numberOfFailedAttempts = targetWc.getNumberOfFailedAttempts();
            int targetAttempts = 3;
            if (numberOfFailedAttempts < targetAttempts) {
                log.debug("Failing batch containing target offset {}", FAILURE_TARGET);
                throw new FakeRuntimeError(msg("Testing failure processing a batch - pretend attempt #{}", numberOfFailedAttempts));
            } else {
                log.debug("Failing target {} now completing as has has reached target attempts {}", offsets, targetAttempts);
            }
        }
        log.debug("Completing batch {}", offsets);
        return null;
    }

}