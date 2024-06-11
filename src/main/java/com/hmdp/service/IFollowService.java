package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Follow;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IFollowService extends IService<Follow> {

    Result isFollow(Long followUserId);

    Result follow(Long followUserId, Boolean isFollow);

    /**
     * 查询两个用户的共同关注博主
     * @param id
     * @return
     */
    Result followCommons(Long id);
}
