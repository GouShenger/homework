package com.lucas.message;

import com.lucas.util.ResourceUtil;
import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * @author : lgs
 * @Description ：
 * @date : 2020/11/3
 */
public class MessageConsumer {
    private final static String QUEUE_NAME = "ORIGIN_QUEUE";
    private final static String TOPIC_QUEUE_NAME = "ORIGIN_TOPIC_QUEUE";
    private final static String FANOUT_QUEUE_NAME = "ORIGIN_FANOUT_QUEUE";

    private final static String DIRECT_EXCHANGE_NAME = "ORIGIN_DIRECT_EXCHANGE";
    private final static String TOPIC_EXCHANGE_NAME = "ORIGIN_TOPIC_EXCHANGE";
    private final static String FANOUT_EXCHANGE_NAME = "ORIGIN_FANOUT_EXCHANGE";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(ResourceUtil.getKey("rabbitmq.uri"));

        // 建立连接
        Connection conn = factory.newConnection();
        // 创建消息通道
        Channel channel = conn.createChannel();



        // 声明队列
        // String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments
        channel.queueDeclare(FANOUT_QUEUE_NAME, false, false, false, null);

        //声明直连交换机 路由键和绑定键完全匹配
//        channel.exchangeDeclare(DIRECT_EXCHANGE_NAME,"direct",false,false,null);
        //声明主题交换机
//        channel.exchangeDeclare(TOPIC_EXCHANGE_NAME,"topic",false,false,null);

        //声明广播交换机
        channel.exchangeDeclare(FANOUT_EXCHANGE_NAME,"fanout",false,false,null);

        //绑定
//        channel.queueBind(QUEUE_NAME,DIRECT_EXCHANGE_NAME,"MX");
        channel.queueBind(FANOUT_QUEUE_NAME,FANOUT_EXCHANGE_NAME,"*.MX.*");

        System.out.println(" Waiting for message....");

        // 创建消费者
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String msg = new String(body, "UTF-8");
                System.out.println("Received message : '" + msg + "' ");
                System.out.println("messageId : " + properties.getMessageId());
                System.out.println(properties.getHeaders().get("name") + " -- " + properties.getHeaders().get("level"));
            }
        };

        // 开始获取消息
        // String queue, boolean autoAck, Consumer callback
        channel.basicConsume(FANOUT_QUEUE_NAME, true, consumer);
    }
}

