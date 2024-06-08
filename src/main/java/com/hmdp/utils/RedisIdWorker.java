package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     *
     * @param prefix ：用于区分不同业务，这样不同业务就有不同的key
     * @return 返回一个64位的uuid
     */
    public  long nextId(String prefix)
    {
        // 生成当前时间戳
        LocalDateTime now = LocalDateTime.now();
        long timeStamp = now.toEpochSecond(ZoneOffset.UTC);

        // 日期是为了将同一个业务按照日期将订单分开，不然redis一个key无限自增会达到极限，最好设置一个24h的过期时间
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + prefix + ":" + date);
        // 返回结果
       return timeStamp << 32 | count;

    }

}
