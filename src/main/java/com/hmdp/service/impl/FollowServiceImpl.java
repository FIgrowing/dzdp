package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;

    /**
     * 判断当前用户是否关注博主
     * @param followUserId
     * @return
     */
    @Override
    public Result isFollow(Long followUserId) {
        // 1.获取登录用户
        Long userId = UserHolder.getUser().getId();
        // 2.查询是否关注 select count(*) from tb_follow where user_id = ? and follow_user_id = ?
        Integer count = query().eq("user_id", userId).eq("follow_user_id", followUserId).count();
        // 3.判断
        return Result.ok(count > 0);
    }

    /**
     * 实现关注和取消关注功能
     * @param followUserId
     * @param isFollow
     * @return
     */
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        // 关注：将当前用户加入该博主的粉丝列表
        String key = "follows:" + userId;
        if (isFollow)
        {
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean isSeccess = save(follow);
            if (isSeccess)
            {
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        }
        // 取消关注：将当前用户从博主的粉丝列表里移除
        else {
            boolean isSeccess = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId).eq("follow_user_id", followUserId));
            if (isSeccess)
            {
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }

    /**
     * 查询两个用户的共同关注博主
     * @param id
     * @return
     */
    @Override
    public Result followCommons(Long id) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 获取用户1，2的关注列表
        String key1 = "follows:" + userId;
        String key2 = "follows:"+id;
        // 利用set数据结构实现求交集，来实现共同关注
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key1, key2);
        if (intersect==null || intersect.isEmpty())
        {
            return  Result.ok(Collections.emptyList());
        }
        // todo 将查询到的共同关注的博主脱敏返回
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        //List<User> userList = userService.list(new QueryWrapper<User>().in("id", ids));
        List<User> users = userService.listByIds(ids);
        List<UserDTO> userDTOS = users.stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);

    }
}
