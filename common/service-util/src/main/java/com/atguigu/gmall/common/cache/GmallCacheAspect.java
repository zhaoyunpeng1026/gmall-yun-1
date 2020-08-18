package com.atguigu.gmall.common.cache;


import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
@Aspect
public class GmallCacheAspect {

    @Autowired
    RedisTemplate redisTemplate;

    @Around("@annotation(com.atguigu.gmall.common.cache.GmallCache)")
    public Object cacheAroundAdvice(ProceedingJoinPoint point) {
        // 缓存处理代码
        Object proceed = null;

        // 拼接缓存key
        Object[] args = point.getArgs();// 通过反射获得被代理方法的参数
        String id = new String(args[0]+"");
        // 获得执行方法的注解，通过注解，判断当前方法，获得缓存前缀
        MethodSignature signature = (MethodSignature) point.getSignature();// 通过反射获得当前要执行的被代理方法信息
        Class returnType = signature.getReturnType();// 通过反射获得方法返回值类型
        GmallCache annotation = signature.getMethod().getAnnotation(GmallCache.class);// 通过反射获得被代理方法注解
        String prefix = annotation.prefix();// 注解，用来存储key的前缀
        String cacheKey = prefix + id;

        // 获得key之后，查询缓存
        proceed = cacheHit(returnType,cacheKey); //(String) redisTemplate.opsForValue().get(cacheKey);

        if (null!=proceed) {
            return proceed;
        } else {
            // 查询db时必须获得分布式锁，以保证数据库操作的安全性
            String uid = UUID.randomUUID().toString();
            Boolean stockLock = redisTemplate.opsForValue().setIfAbsent(cacheKey + ":lock" , uid, 1, TimeUnit.SECONDS);//3秒钟分布式锁过期时间
            if (stockLock) {
                try {
                    proceed = point.proceed();
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
                if(null==proceed){
                    try {
                        redisTemplate.opsForValue().set(cacheKey,returnType.newInstance());
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
                // 同步缓存
                redisTemplate.opsForValue().set(cacheKey,JSON.toJSONString(proceed));

                String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
                // 设置lua脚本返回的数据类型
                DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
                // 设置lua脚本返回类型为Long
                redisScript.setResultType(Long.class);
                redisScript.setScriptText(script);
                redisTemplate.execute(redisScript, Arrays.asList(cacheKey+":lock"),uid);

            }else{
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return cacheHit(returnType,cacheKey);
            }


        }
        return proceed;
    }

    private Object cacheHit(Class returnType, String cacheKey) {
        Object o = null;
        String cacheObject = (String) redisTemplate.opsForValue().get(cacheKey);

        if(StringUtils.isNotBlank(cacheObject)){
            o = JSON.parseObject(cacheObject,returnType);
        }
        return o;
    }


}

