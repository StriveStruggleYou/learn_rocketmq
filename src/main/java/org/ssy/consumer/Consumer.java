package org.ssy.consumer;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.consumer.PullRequest;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
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

//    consumer.getDefaultMQPushConsumerImpl().setConsumeMessageService();

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

    ConsumeMessageConcurrentlyService consumeMessageConcurrentlyService = (ConsumeMessageConcurrentlyService) consumer
        .getDefaultMQPushConsumerImpl().getConsumeMessageService();

//    consumeMessageConcurrentlyService.

//
//    MQClientManager manager = MQClientManager.getInstance();
//
//
//    pullMessageService.

    TimerTask timerTask = new TimerTask() {
      int flag = 0;


      @Override
      public void run() {
        PullMessageService pullMessageService = consumer.getDefaultMQPushConsumerImpl()
            .getmQClientFactory().getPullMessageService();
        LinkedBlockingQueue<PullRequest> pullRequestQueue = null;

        Class cls = pullMessageService.getClass();

        Field[] fields = cls.getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {//遍历
          try {
            //得到属性
            Field field = fields[i];
            //打开私有访问
            field.setAccessible(true);
            //获取属性
            String name = field.getName();

            if (name.equals("pullRequestQueue")) {
              pullRequestQueue = (LinkedBlockingQueue<PullRequest>) field.get(pullMessageService);
              break;
            }
          } catch (IllegalAccessException e) {
            e.printStackTrace();
          }
        }

        if (pullRequestQueue != null && pullRequestQueue.size() > 0) {
          for (PullRequest pullRequest : pullRequestQueue) {
            System.out.println(pullRequest.getProcessQueue().getMsgTreeMap().size());
          }
        }
      }
    };
    Timer timer = new Timer();
    timer.schedule(timerTask, 0, 1000);

    System.out.printf("Consumer Started.%n");
  }
}