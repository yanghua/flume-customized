package com.github.flumecustomized.serialization;

/**
 * Created by yanghua on 11/13/15.
 */

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.LineDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * flume's multi line deserializer
 * 注意: 此类不可直接使用,必须放入flume源码的:org.apache.flume.serialization包路径下,
 * 然后在类:org.apache.flume.serialization.EventDeserializerType 增加枚举 MULTILINE
 * 并重新编译 module :flume-ng-core
 * 将新生成的jar 替换原来的,方可生效
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MultiLineDeserializer implements EventDeserializer {

    private static final Logger logger = LoggerFactory.getLogger
            (LineDeserializer.class);
    private static final Gson   GSON   = new Gson();

    private final    ResettableInputStream in;
    private final    Charset               outputCharset;
    private final    int                   maxLineLength;
    private final    String                newLineStartPrefix;
    private volatile boolean               isOpen;
    private boolean wrappedByDocker = false;

    public static final String OUT_CHARSET_KEY = "outputCharset";
    public static final String CHARSET_DFLT    = "UTF-8";

    public static final String MAXLINE_KEY  = "maxLineLength";
    public static final int    MAXLINE_DFLT = 8192;

    public static final String NEW_LINE_START_PREFIX = "newLineStartPrefix";
    public static final String START_PREFIX_DFLT     = "[";

    public static final String  WRAPPED_BY_DOCKER         = "wrappedByDocker";
    public static final boolean DEFAULT_WRAPPED_BY_DOCKER = false;

    MultiLineDeserializer(Context context, ResettableInputStream in) {
        this.in = in;
        this.outputCharset = Charset.forName(
                context.getString(OUT_CHARSET_KEY, CHARSET_DFLT));
        this.maxLineLength = context.getInteger(MAXLINE_KEY, MAXLINE_DFLT);
        this.newLineStartPrefix = context.getString(NEW_LINE_START_PREFIX, START_PREFIX_DFLT);
        this.wrappedByDocker = context.getBoolean(WRAPPED_BY_DOCKER, DEFAULT_WRAPPED_BY_DOCKER);
        this.isOpen = true;
    }

    /**
     * Reads a line from a file and returns an event
     *
     * @return Event containing parsed line
     * @throws IOException
     */
    @Override
    public Event readEvent() throws IOException {
        ensureOpen();
        String line = readLine();
        if (line == null) {
            return null;
        } else {
            return EventBuilder.withBody(line, outputCharset);
        }
    }

    /**
     * Batch line read
     *
     * @param numEvents Maximum number of events to return.
     * @return List of events containing read lines
     * @throws IOException
     */
    @Override
    public List<Event> readEvents(int numEvents) throws IOException {
        ensureOpen();
        List<Event> events = Lists.newLinkedList();
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent();
            if (event != null) {
                events.add(event);
            } else {
                break;
            }
        }
        return events;
    }

    @Override
    public void mark() throws IOException {
        ensureOpen();
        in.mark();
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
        in.reset();
    }

    @Override
    public void close() throws IOException {
        if (isOpen) {
            reset();
            in.close();
            isOpen = false;
        }
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw new IllegalStateException("Serializer has been closed");
        }
    }

    // TODO: consider not returning a final character that is a high surrogate
    // when truncating
    private String readLine() throws IOException {
        int           lineNum = 0;
        int           charNum = 0;
        long          readBeforeOffset;
        StringBuilder sb      = new StringBuilder();

        do {
            readBeforeOffset = in.tell();
            String preReadLine = readSingleLine();

            if (preReadLine == null) return null;

            //if the log is wrapped by docker log format,
            //should extract origin log firstly
            if (wrappedByDocker) {
                DockerLog dockerLog = GSON.fromJson(preReadLine, DockerLog.class);
                preReadLine = dockerLog.getLog();
            }

            lineNum++;
            charNum += preReadLine.length();

            if (charNum >= this.maxLineLength) {
                logger.warn("Line length exceeds max ({}), truncating line!",
                        maxLineLength);
                break;
            }

            if (lineNum > 1 && preReadLine.startsWith(newLineStartPrefix)) {
                //roll back
                in.seek(readBeforeOffset);

                break;
            }

            sb.append(preReadLine);

        } while (true);

        return sb.toString();
    }

    private String readSingleLine() throws IOException {
        StringBuilder sb        = new StringBuilder();
        int           c;
        int           readChars = 0;
        while ((c = in.readChar()) != -1) {
            readChars++;

            // FIXME: support \r\n
            if (c == '\n') {
                break;
            }

            sb.append((char) c);
        }

        if (readChars > 0) {
            return sb.toString();
        } else {
            return null;
        }
    }

    public static class Builder implements EventDeserializer.Builder {

        @Override
        public EventDeserializer build(Context context, ResettableInputStream in) {
            return new MultiLineDeserializer(context, in);
        }

    }

    public static class DockerLog {

        private String log;
        private String stream;
        private String time;

        public DockerLog() {
        }

        public String getLog() {
            return log;
        }

        public void setLog(String log) {
            this.log = log;
        }

        public String getStream() {
            return stream;
        }

        public void setStream(String stream) {
            this.stream = stream;
        }

        public String getTime() {
            return time;
        }

        public void setTime(String time) {
            this.time = time;
        }
    }

}
