/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class Consumer {

    public static final String CONSUMER_GROUP = "please_rename_unique_group_name_4";
    public static final String DEFAULT_NAMESRVADDR = "192.168.33.122:9876";
    public static final String TOPIC = "TopicTest";

    public static void main(String[] args) throws MQClientException {

        /*
         * Instantiate with specified consumer group name.
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);

        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */
        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
         consumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        /*
         * Specify where to start in case the specific consumer group is a brand-new one.
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        /*
         * Subscribe one more topic to consume.
         */
        consumer.subscribe(TOPIC, "*");
        /*
         *  Register callback to execute on arrival of messages fetched from brokers.
         */
        // 顺序消费
//        consumer.registerMessageListener((MessageListenerOrderly) (msg, context) -> {
//            for (int i = 0; i < msg.size(); i++) {
//                MessageExt messageExt = msg.get(i);
//                if (messageExt.getQueueId() == 0) {
//                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(messageExt.getBody()));
//                    try {
//                        Thread.sleep(1000);
//                        System.out.println("等待。。。。");
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//            }
//            return ConsumeOrderlyStatus.SUCCESS;
//        });

        // 核心配置：设置每个批次的最大消费数量为 5
        consumer.setConsumeMessageBatchMaxSize(3);

        // 可选：配置拉取相关参数（辅助控制批次）
        // 1. 每次从 Broker 拉取的最大消息数（默认 32，建议 >= consumeMessageBatchMaxSize）
        consumer.setPullBatchSize(3);
        // 2. 消费线程数（根据批次大小调整，比如批次5，线程数2，总并发处理10条）
        consumer.setConsumeThreadMin(2);
        consumer.setConsumeThreadMax(2);

//        AtomicBoolean test = new AtomicBoolean(true);
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            for (int i = 0; i < msg.size(); i++) {
                MessageExt messageExt = msg.get(i);
                String body = new String(messageExt.getBody());
                try {
                    System.out.println("等待一秒" + i);
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
//                if (body.contains("0") && test.get()) {
//                    System.out.println("返回一个错误,mq重试");
//                    System.out.printf("%s Receive New false Messages: %s %n", Thread.currentThread().getName(), body);
//                    test.set(false);
//                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
//                }
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), body);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        /*
         *  Launch the consumer instance.
         */
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
