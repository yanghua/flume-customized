package com.github.flumecustomized.source;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.github.flumecustomized.source.SpoolFileSourceConfigConstants.*;

/**
 * Created by yanghua on 6/1/15.
 */
public class SpoolFileSource extends AbstractSource implements Configurable, EventDrivenSource {

    private static final Logger logger = LoggerFactory.getLogger(SpoolFileSource.class);

    // Delay used when polling for new files
    private static final int POLL_DELAY_MS = 500;

    /* Config options */
    private String            completedSuffix;
    private String            spoolDirectory;
    private boolean           fileHeader;
    private String            fileHeaderKey;
    private boolean           basenameHeader;
    private String            basenameHeaderKey;
    private int               batchSize;
    private String            ignorePattern;
    private String            targetPattern;
    private String            targetFilename;
    private String            targetYoungestFileName;
    private String            trackerDirPath;
    private String            deserializerType;
    private Context           deserializerContext;
    private String            deletePolicy;
    private String            inputCharset;
    private DecodeErrorPolicy decodeErrorPolicy;
    private volatile boolean hasFatalError = false;

    private SourceCounter sourceCounter;
    ReliableSpoolFileEventReader reader;
    private ScheduledExecutorService executor;
    private boolean backoff             = true;
    private boolean hitChannelException = false;
    private int          maxBackoff;
    private ConsumeOrder consumeOrder;

    @Override
    public synchronized void start() {
        logger.info("SpoolDirectoryTailFileSource source starting with directory: {}, targetFilename: {}", spoolDirectory, targetFilename);

        executor = Executors.newSingleThreadScheduledExecutor();
        File directory = new File(spoolDirectory);

        try {
            reader = (new ReliableSpoolFileEventReader.Builder())
                .spoolDirectory(directory)
                .completedSuffix(completedSuffix)
                .ignorePattern(ignorePattern)
                .targetPattern(targetPattern)
                .targetYoungestFileName(targetYoungestFileName)
                .targetFilename(targetFilename)
                .trackerDirPath(trackerDirPath)
                .annotateFileName(fileHeader)
                .fileNameHeader(fileHeaderKey)
                .annotateBaseName(basenameHeader)
                .baseNameHeader(basenameHeaderKey)
                .deserializerType(deserializerType)
                .deserializerContext(deserializerContext)
                .deletePolicy(deletePolicy)
                .inputCharset(inputCharset)
                .decodeErrorPolicy(decodeErrorPolicy)
                .consumeOrder(consumeOrder)
                .build();
        } catch (IOException e) {
            throw new FlumeException("Error instantiating spooling and tail event parser", e);
        }

        Runnable runner = new SpoolFileRunnable(reader, sourceCounter);
        executor.scheduleWithFixedDelay(runner, 0, POLL_DELAY_MS, TimeUnit.MILLISECONDS);

        super.start();
        logger.debug("SpoolDirectoryTailFileSource source started");
        sourceCounter.start();

    }

    @Override
    public synchronized void stop() {
        executor.shutdown();
        try {
            executor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.info("Interrupted while awaiting termination", e);
        }
        executor.shutdownNow();

        super.stop();
        sourceCounter.stop();
        logger.info("SpoolDirTailFile source {} stopped. Metrics: {}", getName(), sourceCounter);
    }


    @Override
    public String toString() {
        return "Spool Directory Tail File source " + getName() + ": { spoolDir: " + spoolDirectory + ", targetFilename: " + targetFilename + " }";
    }

    @Override
    public synchronized void configure(Context context) {
        spoolDirectory = context.getString(SPOOL_DIRECTORY);
        Preconditions.checkState(spoolDirectory != null, "Configuration must specify a spooling directory");

        completedSuffix = context.getString(SPOOLED_FILE_SUFFIX, DEFAULT_SPOOLED_FILE_SUFFIX);
        deletePolicy = context.getString(DELETE_POLICY, DEFAULT_DELETE_POLICY);

        fileHeader = context.getBoolean(FILENAME_HEADER, DEFAULT_FILENAME_HEADER);
        fileHeaderKey = context.getString(FILENAME_HEADER_KEY, DEFAULT_FILENAME_HEADER_KEY);

        basenameHeader = context.getBoolean(BASENAME_HEADER, DEFAULT_BASENAME_HEADER);
        basenameHeaderKey = context.getString(BASENAME_HEADER_KEY, DEFAULT_BASENAME_HEADER_KEY);

        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        inputCharset = context.getString(INPUT_CHARSET, DEFAULT_INPUT_CHARSET);

        decodeErrorPolicy = DecodeErrorPolicy.valueOf(context.getString(DECODE_ERROR_POLICY, DEFAULT_DECODE_ERROR_POLICY).toUpperCase());
        ignorePattern = context.getString(IGNORE_PAT, DEFAULT_IGNORE_PAT);
        targetPattern = context.getString(TARGET_PAT, DEFAULT_TARGET_PAT);
        targetYoungestFileName = context.getString(TARGET_YOUNGEST_FILENAME, DEFAULT_TARGET_YUNEST_FILENAME);
        targetFilename = context.getString(TARGET_FILENAME, DEFAULT_TARGET_FILENAME);
        trackerDirPath = context.getString(TRACKER_DIR, DEFAULT_TRACKER_DIR);

        deserializerType = context.getString(DESERIALIZER, DEFAULT_DESERIALIZER);
        deserializerContext = new Context(context.getSubProperties(DESERIALIZER + "."));

        consumeOrder = SpoolFileSourceConfigConstants.ConsumeOrder.valueOf(context.getString(CONSUME_ORDER, DEFAULT_CONSUME_ORDER.toString()).toUpperCase());

        maxBackoff = context.getInteger(MAX_BACKOFF, DEFAULT_MAX_BACKOFF);

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }

    }

    private class SpoolFileRunnable implements Runnable {
        private ReliableSpoolFileEventReader reader;
        private SourceCounter                sourceCounter;

        public SpoolFileRunnable(ReliableSpoolFileEventReader reader, SourceCounter sourceCounter) {
            this.reader = reader;
            this.sourceCounter = sourceCounter;
        }

        @Override
        public void run() {
            int backoffInterval = 250;
            try {
                while (!Thread.interrupted()) {
                    List<Event> events = reader.readEvents(batchSize);
                    if (events.isEmpty()) {
                        reader.commit();    // Avoid IllegalStateException while tailing file.
                        break;
                    }
                    sourceCounter.addToEventReceivedCount(events.size());
                    sourceCounter.incrementAppendBatchReceivedCount();

                    try {
                        getChannelProcessor().processEventBatch(events);
                        reader.commit();
                    } catch (ChannelException ex) {
                        logger.warn("The channel is full, and cannot write data now. The " +
                                        "source will try again after " + String.valueOf(backoffInterval) +
                                        " milliseconds");
                        hitChannelException = true;
                        if (backoff) {
                            TimeUnit.MILLISECONDS.sleep(backoffInterval);
                            backoffInterval = backoffInterval << 1;
                            backoffInterval = backoffInterval >= maxBackoff ? maxBackoff :
                                              backoffInterval;
                        }
                        continue;
                    }
                    backoffInterval = 250;
                    sourceCounter.addToEventAcceptedCount(events.size());
                    sourceCounter.incrementAppendBatchAcceptedCount();
                }
                // logger.info("Spooling Directory Tail File Source runner has shutdown.");
            } catch (Throwable t) {
                logger.error("FATAL: " + SpoolFileSource.this.toString() + ": " +
                                 "Uncaught exception in SpoolDirectoryTailSourceSource thread. " +
                                 "Restart or reconfigure Flume to continue processing.", t);
                hasFatalError = true;
                Throwables.propagate(t);
            }
        }
    }

}
