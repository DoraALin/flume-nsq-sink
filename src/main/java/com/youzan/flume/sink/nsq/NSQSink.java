package com.youzan.flume.sink.nsq;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by lin on 17/4/6.
 */
public class NSQSink extends AbstractSink implements Configurable {

    private final static Logger logger = LoggerFactory.getLogger(NSQSink.class);

    private String lookupAddresses = null;
    private String defaultTopic = null;
    private String backupPath;
    private String scopeId;
    private String backupContext;
    private NSQConfig config = null;
    private Producer producer = null;
    private NSQLocalBackupQueue backupQueue = null;
    private ExecutorService exec = Executors.newSingleThreadExecutor();
    private final Runnable READTASK = () -> {
        while(true) {
            byte[] message = null;
            try {
                message = backupQueue.readBackup();
                if(null != message) {
                    logger.info("Read one backup message.");
                    producer.publish(message, defaultTopic);
                    logger.info("backup message published.");
                }
            } catch (IOException e) {
                logger.error("Fail to read from backup file");
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for notify.");
            } catch (NSQException e) {
                logger.error("Fail to publish to NSQd, write back to backup.");
                try {
                    backupQueue.writeBackup(message);
                } catch (IOException e1) {
                    logger.error("Fail to write to backup queue after read out from backup. Message {}", new String(message));
                }
            }
        }
    };

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        String topic;
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        Event event = ch.take();
        byte[] message = event.getBody();
        try{
            String topicText = event.getHeaders().get("topic");
            if(null != topicText)
                topic = topicText;
            else
                topic = defaultTopic;
            //send
            producer.publish(message, topic);
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            logger.error("Fail to publish to NSQ.", t);
            try {
                backupQueue.writeBackup(message);
                logger.warn("Failed message write to backup file.");
                txn.commit();
                status = Status.READY;
            } catch (IOException e) {
                logger.error("Fail to write to backup queue. Message {}", new String(message));
                txn.rollback();
                status = Status.BACKOFF;
            }
            if (t instanceof Error) {
                throw (Error)t;
            }
        }finally {
            txn.close();
        }
        return status;
    }

    @Override
    public void start() {
        producer = new ProducerImplV2(config);
        try {
            producer.start();
        } catch (NSQException e) {
            logger.error("Fail to start nsq producer.", e);
        }
        backupQueue = new NSQLocalBackupQueue(backupPath, scopeId, backupContext);
        final NSQLocalBackupQueue queue = this.backupQueue;
        exec.submit(READTASK);
    }

    @Override
    public void stop () {
        producer.close();
        exec.shutdown();
        try {
            backupQueue.close();
        } catch (IOException e) {
            logger.error("Fail to close backup queue");
        }
    }

    @Override
    public void configure(Context context) {
        lookupAddresses = context.getString("lookupdAddresses");
        if(null == lookupAddresses || lookupAddresses.isEmpty())
            throw  new IllegalArgumentException("Illegal lookupd addresses not accepted. " + lookupAddresses);
        String topicName = context.getString("topic");
        if (null == topicName || topicName.isEmpty()) {
            throw new IllegalArgumentException("Illegal default topic name is not accepted. " + topicName);
        }
        defaultTopic = topicName;
        config = new NSQConfig();
        config.setLookupAddresses(lookupAddresses);
        logger.info("NSQConfig initialized with lookupAddresses:{}, topicName:{}", lookupAddresses, topicName);

        backupPath = context.getString("backupPath");
        scopeId = context.getString("scopeId");
        backupContext = context.getString("backupContext");
    }
}
