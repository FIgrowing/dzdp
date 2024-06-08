package com.hmdp.service.impl;

import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private CacheClient cacheClient;


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {

        Shop shop =null;

        String key = "cache:shop:"+id;
        // 从redis中查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 存在则直接返回
        if (StrUtil.isNotBlank(shopJson))
        {
            shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
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
                shop = query().eq("id", id).one();
                // 数据库也不存在则直接返回
                if (shop==null)
                {
                    //todo 如果数据库也没有则在redis中写入null值,减少缓存穿透
                    stringRedisTemplate.opsForValue().set(key,"", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                    return Result.fail("店铺不存在");
                }
                int randomExTime = RandomUtil.randomInt(0, 24);
                // todo 数据库存在则回写redis，并且设置一个随机过期时间，防止缓存雪崩
                stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),randomExTime,TimeUnit.HOURS);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            // todo 无论是否出现异常都要释放锁
            unLock(lockKey);
        }
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if (id==null)
        {
            return Result.fail("店铺id不能为空");
        }
        // 先更新数据库
        updateById(shop);
        // 再删除缓存
        stringRedisTemplate.delete("cache:shop:"+id);
        return Result.ok();
    }

//    public Result query(Long id)
//    {
//
//        Shop shop = cacheClient.queryBySafe("cache:shop:", id, Shop.class, id2 -> getById(id2), 100, TimeUnit.SECONDS);
//        return Result.ok(shop);
//    }


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
