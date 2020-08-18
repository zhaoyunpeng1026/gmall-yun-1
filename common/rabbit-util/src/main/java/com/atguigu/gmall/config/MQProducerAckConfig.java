package com.atguigu.gmall.config;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.constant.MqConst;
import com.atguigu.gmall.entity.GmallCorrelationData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;

@Component
@Slf4j
public class MQProducerAckConfig implements RabbitTemplate.ConfirmCallback,RabbitTemplate.ReturnCallback {


    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RedisTemplate redisTemplate;

    @PostConstruct
    void init(){
        rabbitTemplate.setReturnCallback(this);
        rabbitTemplate.setConfirmCallback(this);
    }

    @Override
    public void confirm(@org.springframework.lang.Nullable CorrelationData correlationData, boolean ack, @Nullable String cause) {

        System.out.println(ack);
        System.out.println("消息发送给交换机");

        if(!ack){
            // 将发送失败的消息信息放入缓存，等待补偿
            String id = correlationData.getId();
            GmallCorrelationData gmallCorrelationDataFromCache = JSON.parseObject((String)redisTemplate.opsForValue().get(id),GmallCorrelationData.class);
            addRetry(gmallCorrelationDataFromCache);
        }

    }

    @Override
    public void returnedMessage(Message message, int replyCode, String replyText, String exchange, String routingKey) {

        System.out.println("消息在交换机上投递失败");

        // 将投递失败的消息信息放入缓存，等待补偿
        String id = (String)message.getMessageProperties().getHeaders().get("spring_returned_message_correlation");
        GmallCorrelationData gmallCorrelationDataFromCache = JSON.parseObject((String)redisTemplate.opsForValue().get(id),GmallCorrelationData.class);
        addRetry(gmallCorrelationDataFromCache);
    }


    /**
     * 添加重试
     * @param correlationData
     */
    private void addRetry(CorrelationData correlationData) {
        GmallCorrelationData gmallCorrelationData = (GmallCorrelationData)correlationData;
        int retryCount = gmallCorrelationData.getRetryCount();
        if(retryCount>= MqConst.RETRY_COUNT) {
            System.out.println("补偿次数用尽，不再重新发送消息："+ JSON.toJSONString(correlationData));
            // 将失败的消息文本记录到日志库中
            // 从redis缓存中删除原始文本
            redisTemplate.delete(gmallCorrelationData.getId());
        } else {
            retryCount += 1;
            gmallCorrelationData.setRetryCount(retryCount);
            redisTemplate.opsForList().leftPush("mq:list", correlationData.getId()+"");
            //次数更新
            redisTemplate.opsForValue().set(gmallCorrelationData.getId(), JSON.toJSONString(correlationData));
        }
    }
}
