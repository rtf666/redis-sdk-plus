package com.rtf.redis.client.lb;

/**
 * redis服务列表
 * @Author : liupeng
 * @Date : 2020-02-10
 * @Modified By
 */
public interface AppRedisHostList {
    /**
     * 获取更新的redis服务列表
     * @return
     */
    String getUpdatedHostList() ;

}
