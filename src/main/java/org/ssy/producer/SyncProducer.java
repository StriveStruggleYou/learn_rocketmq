package org.ssy.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * Created by manager on 2018/7/24.
 */
public class SyncProducer {


  public static void main(String[] args) throws Exception {
    //Instantiate with a org.ssy.producer group name.
    DefaultMQProducer producer = new
        DefaultMQProducer("test_group");

    producer.setNamesrvAddr("127.0.0.1:9876");

    //Launch the instance.
    producer.start();
    for (int i = 0; i < 200; i++) {
      //Create a message instance, specifying topic, tag and message body.
      Message msg = new Message("TopicTest" /* Topic */,
          "TagA" /* Tag */,
          ("Hello RocketMQ " +
              i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
      );

      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      //Call send message to deliver message to one of brokers.
      SendResult sendResult = producer.send(msg);
      System.out.printf("%s%n", sendResult);
    }
    //Shut down once the org.ssy.producer instance is not longer in use.
    producer.shutdown();
  }
}
