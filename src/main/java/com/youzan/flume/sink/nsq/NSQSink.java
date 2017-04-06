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

/**
 * Created by lin on 17/4/6.
 */
public class NSQSink extends AbstractSink implements Configurable {

    private final static Logger logger = LoggerFactory.getLogger(NSQSink.class);

    private String lookupAddresses = null;
    private String defaultTopic = null;
    private NSQConfig config = null;
    private Producer producer = null;

    @Override
    public Status process() throws EventDeliveryException {
        Status status;
        String topic;
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try{
            Event event = ch.take();
            String topicText = event.getHeaders().get("topic");
            if(null != topicText)
                topic = topicText;
            else
                topic = defaultTopic;
            //send
            producer.publish(event.getBody(), topic);
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            logger.error("Fail to publish to NSQ.", t);
            txn.rollback();
            status = Status.BACKOFF;
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
    }

    @Override
    public void stop () {
        producer.close();
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
    }
}
