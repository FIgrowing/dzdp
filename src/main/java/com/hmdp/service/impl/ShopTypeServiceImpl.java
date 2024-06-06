package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public List<ShopType> queryByList() {
        String key = "cache:shopType";
        // 先查询redis
        List<String> shopTypeJsonList = stringRedisTemplate.opsForList().range(key, 0,9);
        // 如果redis存在则直接返回
        if (shopTypeJsonList!=null && shopTypeJsonList.size()==10)
        {
            List<ShopType> shopTypeList = shopTypeJsonList.stream().
                    map(json -> JSONUtil.toBean(json, ShopType.class)).
                    collect(Collectors.toList());
            return shopTypeList;
        }
        // redis不存在则查询数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();
        // 数据库也没有则直接返回
        if (typeList==null)
        {
            return null;
        }
        // 查询万数据库后回写redis
        List<String> shopTypeJson = typeList.stream().
                map(shopType -> JSONUtil.toJsonStr(shopType)).
                collect(Collectors.toList());
        stringRedisTemplate.opsForList().rightPushAll(key, shopTypeJson);
        // 返回结果
        return typeList;
    }
}
