package com.rtf.redis.client.lb;

import lombok.Setter;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;

/**
 * 默认的redis服务更新列表
 * @Author : liupeng
 * @Date : 2020-02-10
 * @Modified By
 */
public class AppRedisHostListDefault implements AppRedisHostList {

    @Setter
    private RedisProperties properties ;

    @Override
    public String getUpdatedHostList() {

        return properties.getHost() ;
    }
}
