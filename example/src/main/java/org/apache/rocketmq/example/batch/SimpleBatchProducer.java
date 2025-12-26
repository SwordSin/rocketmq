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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.example.batch;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.example.commom.Dict;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class SimpleBatchProducer {

    public static final String PRODUCER_GROUP = "FIFOGroup";
    public static final String DEFAULT_NAMESRVADDR = Dict.nameserver;
    public static final String TOPIC = "FIFOTopic";
    public static final String TAG = "tag1";

//    public static void main(String[] args) throws Exception {
//        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP, Dict.getRPCHook());
//        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
//        producer.start();
//
//        List<Message> messages = new ArrayList<>();
//        messages.add(new Message(TOPIC, TAG, "OrderID001", "Hello world 0".getBytes(StandardCharsets.UTF_8)));
//        messages.add(new Message(TOPIC, TAG, "OrderID002", "Hello world 1".getBytes(StandardCharsets.UTF_8)));
//        messages.add(new Message(TOPIC, TAG, "OrderID003", "Hello world 2".getBytes(StandardCharsets.UTF_8)));
//
//        SendResult sendResult = producer.send(messages);
//        System.out.printf("%s", sendResult);
//    }

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer(PRODUCER_GROUP, Dict.getRPCHook());
        defaultMQProducer.setNamesrvAddr(Dict.nameserver);
        defaultMQProducer.setEnableTrace(true);
        // 设置同步发送重试次数为 3 次（不包含初始发送，总计 4 次）
        defaultMQProducer.setRetryTimesWhenSendFailed(3);
        // 设置发送超时时间（单位：毫秒），超时也会触发重试
        defaultMQProducer.setSendMsgTimeout(3000);
        defaultMQProducer.start();
        for (int i = 0; i < 10; i++) {
            int order = i % 4;
            String msg = "hello rocketmq: " + i;
            Message message = new Message(TOPIC, "TagA", String.valueOf(i), msg.getBytes(StandardCharsets.UTF_8));
            SendResult send = defaultMQProducer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer id = (Integer) arg;
                    int index = id % mqs.size();
                    return mqs.get(index);
                }
            }, order);
            if (send.getSendStatus() != SendStatus.SEND_OK) {
            }
            System.out.printf("%s, %s,%s%n", msg, send.getMsgId(), send.getMessageQueue().getQueueId());
        }
        defaultMQProducer.shutdown();
    }
}
