package com.lucas.message;

import com.lucas.util.ResourceUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author : lgs
 * @Description ：
 * @date : 2020/11/3
 */
public class MessageProducer {
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

        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("name", "miaoxiang");
        headers.put("level", "37");

        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .deliveryMode(2)   // 2代表持久化
                .contentEncoding("UTF-8")  // 编码
                .expiration("10000")  // TTL，过期时间
                .headers(headers) // 自定义属性
                .priority(5) // 优先级，默认为5，配合队列的 x-max-priority 属性使用
                .messageId(String.valueOf(UUID.randomUUID()))
                .build();

//        String msg = "Hello world, Rabbit MQ";
        String msg = "Hello  MXsanqi";
        // 声明队列
        // String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments
//        channel.queueDeclare(TOPIC_QUEUE_NAME, false, false, false, null);
        channel.queueDeclare(FANOUT_QUEUE_NAME, false, false, false, null);



        //声明主题交换机
//        channel.exchangeDeclare(TOPIC_EXCHANGE_NAME,"topic",false,false,null);
        //声明广播交换机
        channel.exchangeDeclare(FANOUT_EXCHANGE_NAME,"fanout",false,false,null);

        //声明直连交换机 路由键和绑定键完全匹配
//        channel.exchangeDeclare(DIRECT_EXCHANGE_NAME, "direct", false, false, null);

        // 发送消息
        // String exchange, String routingKey, BasicProperties props, byte[] body
//        channel.basicPublish(DIRECT_EXCHANGE_NAME,"MX", properties, msg.getBytes());
        channel.basicPublish(FANOUT_EXCHANGE_NAME,"null", properties, msg.getBytes());

        channel.close();
        conn.close();
    }
}
