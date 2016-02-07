package com.github.flumecustomized.sink;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.messagebus.client.Messagebus;
import com.messagebus.client.MessagebusSinglePool;
import com.messagebus.client.message.model.Message;
import com.messagebus.client.message.model.MessageFactory;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by yanghua on 11/13/15.
 */
public class MessagebusSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(MessagebusSink.class);

    private static final Gson GSON = new Gson();

    private int                  batchSize;
    private String               zkConnectionStr;
    private String               queueSecret;
    private String               produceToken;
    private String               targetQueue;
    private MessagebusSinglePool singlePool;
    private Messagebus           messagebus;
    private List<Message>        messageList;

    @Override
    public synchronized void start() {
        singlePool = new MessagebusSinglePool(zkConnectionStr);
        messagebus = singlePool.getResource();
        super.start();
    }

    @Override
    public synchronized void stop() {
        try {
            singlePool.returnResource(messagebus);
        } catch (Exception e) {
            singlePool.destroy();
        }
        super.stop();
    }

    @Override
    public void configure(Context context) {
        zkConnectionStr = context.getString(MessagebusSinkConstants.ZK_CONNECTION_STR,
                                   MessagebusSinkConstants.DEFAULT_ZK_CONNECTION_STR);

        queueSecret = context.getString(MessagebusSinkConstants.SOURCE_SECRET);
        if (Strings.isNullOrEmpty(queueSecret)) {
            logger.error("the Property '" + MessagebusSinkConstants.SOURCE_SECRET + "' is a MUST key," +
                             " but not be configurated!" );
            throw new RuntimeException("the Property '" + MessagebusSinkConstants.SOURCE_SECRET + "' is a MUST key," +
                                           " but not be configurated!");
        }

        produceToken = context.getString(MessagebusSinkConstants.STREAM_TOKEN);
        if (Strings.isNullOrEmpty(produceToken)) {
            logger.error("the Property '" + MessagebusSinkConstants.STREAM_TOKEN + "' is a MUST key," +
                             " but not be configurated!");
            throw new RuntimeException("the Property '" + MessagebusSinkConstants.STREAM_TOKEN + "' is a MUST key," +
                                           " but not be configurated!");
        }

        targetQueue = context.getString(MessagebusSinkConstants.SINK_NAME);
        if (Strings.isNullOrEmpty(targetQueue)) {
            logger.error("the Property '" + MessagebusSinkConstants.SINK_NAME + "' is a MUST key" +
                             " but not be configurated!");
            throw new RuntimeException("the Property '" + MessagebusSinkConstants.SINK_NAME + "' is a MUST key" +
                                           " but not be configurated!");
        }

        batchSize = context.getInteger(MessagebusSinkConstants.BATCH_SIZE,
                                       MessagebusSinkConstants.DEFAULT_BATCH_SIZE);
        messageList = new ArrayList<>(batchSize);

        if (logger.isDebugEnabled()) {
            logger.debug("zkConnectionStr : " + zkConnectionStr);
            logger.debug("queueSecret : " + queueSecret);
            logger.debug("produceToken : " + produceToken);
            logger.debug("targetQueue : " + targetQueue);
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;

        try {
            long processedEvents = 0;

            transaction = channel.getTransaction();
            transaction.begin();

            messageList.clear();
            for (; processedEvents < batchSize; processedEvents +=1) {
                event = channel.take();

                if (event == null) {
                    break;
                }

                byte[] eventBody = event.getBody();
                Map<String, String> headers = event.getHeaders();

                if (logger.isDebugEnabled()) {
                    logger.debug("{Event} " + new String(eventBody, "UTF-8"));
                    logger.debug("event ${}", processedEvents);
                }

                String eventObjJsonStr = GSON.toJson(event);

                Message aMsg = MessageFactory.createMessage();
                aMsg.setContent(eventObjJsonStr.getBytes());
                messageList.add(aMsg);
            }

            if (processedEvents > 0) {
                messagebus.batchProduce(queueSecret, targetQueue,
                                        messageList.toArray(new Message[messageList.size()]),
                                        produceToken);
            }

            transaction.commit();
        } catch (Exception ex) {
            String errorMsg = "Failed to publish events";
            logger.error("Failed to publish events", ex);
            result = Status.BACKOFF;
            if (transaction != null) {
                try {
                    transaction.rollback();
                } catch (Exception e) {
                    logger.error("Transaction rollback failed", e);
                    throw Throwables.propagate(e);
                }
            }
            throw new EventDeliveryException(errorMsg, ex);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }

        return result;
    }
}
