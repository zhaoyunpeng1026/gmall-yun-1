package com.atguigu.gmall.service;


import com.atguigu.gmall.entity.GmallCorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class RabbitService {

    @Autowired
    RabbitTemplate rabbitTemplate;


    /**
     * 发送消息
     *
     * @param exchange      交换机
     * @param routingKey    路由键
     * @param message       消息
     */
    public boolean sendMessage(String exchange,String routingKey, Object message){
        GmallCorrelationData gmallCorrelationData = new GmallCorrelationData();
        gmallCorrelationData.setId(UUID.randomUUID().toString().replaceAll("-", ""));
        gmallCorrelationData.setMessage(message);
        gmallCorrelationData.setRoutingKey(routingKey);
        gmallCorrelationData.setExchange(exchange);
        //先在缓存中保存补偿文本
        rabbitTemplate.convertAndSend(exchange,routingKey, message);
        //再发送消息队列，否则在回调函数中无法获取补偿文本
        rabbitTemplate.convertAndSend(exchange,routingKey,message,gmallCorrelationData);

        return true;
    }
}
