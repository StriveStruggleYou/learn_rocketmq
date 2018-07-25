package org.ssy.consumer;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

public class Consumer {

  public static void main(String[] args) throws InterruptedException, MQClientException {

    final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_group");

    consumer.setNamesrvAddr("127.0.0.1:9876");

    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);

    consumer.setPullThresholdForQueue(5);

    consumer.setConsumeTimestamp("20180725092200");

    consumer.subscribe("TopicTest", "*");

    final AtomicInteger integer = new AtomicInteger(0);

    consumer.registerMessageListener(new MessageListenerConcurrently() {


      @Override
      public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
          ConsumeConcurrentlyContext context) {
        System.out.printf(integer.addAndGet(1) + "%s Receive New Messages: %s %n",
            Thread.currentThread().getName(),
            msgs + " size:" + msgs.size());
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
      }
    });

    consumer.start();

    //停止消费
    TimerTask timerTask = new TimerTask() {
      @Override
      public void run() {
        if (integer.get()>10) {
          consumer.shutdown();
          consumer.getDefaultMQPushConsumerImpl().persistConsumerOffset();
        }
      }
    };

    Timer timer = new Timer();
    timer.schedule(timerTask, 0,10);

    System.out.printf("Consumer Started.%n");
  }
}