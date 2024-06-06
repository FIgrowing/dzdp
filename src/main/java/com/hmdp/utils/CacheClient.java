package com.hmdp.utils;

import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RTimeSeries;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IShopService iShopService;

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }


    public <R,ID> R querryCacheWithPassThrough(String prefix, ID id, Class<R> type, Function<ID, R> dbFallBack,Long time, TimeUnit unit)
    {
        R r =null;
        String key = prefix+id;
        // 从redis中查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 存在则直接返回
        if (StrUtil.isNotBlank(shopJson))
        {
            r = JSONUtil.toBean(shopJson, type);
            return r;
        }

        r = dbFallBack.apply(id);
        if (r==null)
        {
            stringRedisTemplate.opsForValue().set(key,"", time, unit);
            return null;
        }
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(r), time,unit);
        return r;
    }

    public <R,ID> R queryBySafe(String prefix, ID id, Class<R> type, Function<ID,R> dbFallBack, Long time, TimeUnit unit) {

        R r =null;

        String key = prefix+id;
        // 从redis中查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 存在则直接返回
        if (StrUtil.isNotBlank(shopJson))
        {
            r = JSONUtil.toBean(shopJson, type);
            return r;
        }

        String lockKey = "cache:lock:"+id;
        try {
            // todo 通过设置互斥锁来缓解缓存击穿问题
            while (tryLock(lockKey))
            {
                // todo 如果之前已经有线程回写redis，那么后续拿到锁的线程可以直接返回了，不需要继续往下执行
                String flag = stringRedisTemplate.opsForValue().get(key);
                if (StrUtil.isNotBlank(flag))
                {
                    break;
                }
                // 不存在则查询数据库
                r = dbFallBack.apply(id);
                // 数据库也不存在则直接返回
                if (r==null)
                {
                    //todo 如果数据库也没有则在redis中写入null值,减少缓存穿透
                    stringRedisTemplate.opsForValue().set(key,"", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                    return null;
                }
                int randomExTime = RandomUtil.randomInt(0, 24);
                // todo 数据库存在则回写redis，并且设置一个随机过期时间，防止缓存雪崩
                stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(r),time,unit);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            // todo 无论是否出现异常都要释放锁
            unLock(lockKey);
        }
        return r;
    }

    private Boolean tryLock(String key)
    {
        Boolean isLock = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return isLock;
    }
    private void unLock(String key)
    {
        stringRedisTemplate.delete(key);
    }

}
