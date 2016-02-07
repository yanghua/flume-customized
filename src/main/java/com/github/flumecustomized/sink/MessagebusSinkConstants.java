package com.github.flumecustomized.sink;

/**
 * Created by yanghua on 11/13/15.
 */
public class MessagebusSinkConstants {

    public static final String PROPERTY_PREFIX = "mb.";

    public static final String BATCH_SIZE         = "batchSize";
    public static final int    DEFAULT_BATCH_SIZE = 100;

    public static final String ZK_CONNECTION_STR         = "zkConnectionStr";
    public static final String DEFAULT_ZK_CONNECTION_STR = "127.0.0.1:2181";

    public static final String SOURCE_SECRET = "sourceSecret";
    public static final String STREAM_TOKEN  = "streamToken";
    public static final String SINK_NAME     = "sinkName";

}
