package com.rtf.redis.client.lb;

import com.rtf.redis.client.AppRedisConnectionFactory;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * redis连接工厂循环规则
 * @Author : liupeng
 * @Date : 2020-02-10
 * @Modified By
 */
@Slf4j
public class AppRedisRoundRobinRule {

    /**
     * redis连接工厂最大选择次数
     */
    private static Integer MAX_SELECT_COUNT = 5 ;

    private List<AppRedisConnectionFactory> connectionFactories = Lists.newArrayList() ;

    private AtomicInteger nextMasterCyclicCounter ;

    private AtomicInteger nextSlaveCyclicCounter;

    public AppRedisRoundRobinRule( List<AppRedisConnectionFactory> connectionFactories ){
        this.connectionFactories = connectionFactories ;

        nextMasterCyclicCounter = new AtomicInteger(0) ;

        nextSlaveCyclicCounter = new AtomicInteger(0) ;
    }

    /**
     * 选择可用的redis连接工厂
     * @param key
     * @return
     */
    public AppRedisConnectionFactory choose(Object key) {
        // 1. 选择主节点
        AppRedisConnectionFactory connectionFactory = choose( key , true ) ;

        // 2. 选择从节点
        if( connectionFactory == null ){
            connectionFactory = choose( key , false ) ;
        }

        return connectionFactory ;
    }

    /**
     * 选择可用的redis连接工厂
     * @param key
     * @return
     */
    public AppRedisConnectionFactory choose(Object key , boolean useMaster) {
        if (connectionFactories == null || connectionFactories.size()<0) {
            log.warn("无可用的redis连接池供选择,useMaster={}, {}" , useMaster , key);
            return null;
        }

        // 筛选指定类型的连接工厂
        List<AppRedisConnectionFactory> targetConnectionFactories = connectionFactories.stream()
                .filter( item -> useMaster ? item.isMaster() : !item.isMaster() )
                .collect(Collectors.toList()) ;

        AppRedisConnectionFactory connectionFactory = null ;
        int count = 0 ;
        // 未找到连接工厂并且选取次数小于10
        while ( connectionFactory==null && count++ < MAX_SELECT_COUNT && targetConnectionFactories.size()>0 ){
            int nextServerIndex = incrementAndGetModulo( useMaster ? nextMasterCyclicCounter : nextSlaveCyclicCounter ,
                    targetConnectionFactories.size() ) ;
            try{
                connectionFactory = targetConnectionFactories.get( nextServerIndex ) ;
                //检查连接池是否熔断
                AppRedisCircuitBreaker appRedisCircuitBreaker = AppRedisCircuitBreaker.getInstance( connectionFactory.getHostName() ) ;
                if( appRedisCircuitBreaker.isCircuitBreakerTripped() ){
                    log.debug("redis主机:{}处于熔断状态" , connectionFactory.getHostName()); ;
                    connectionFactory = null ;
                    continue;
                }
            }catch( Exception e ){
                log.error( "选取redis连接工厂异常 : {}" , e.getMessage() ) ;
            }
        }

        return connectionFactory ;
    }

    /**
     * 选取下一个连接池索引号，不能超过连接池总数量的索引号
     * @param cyclicCounter
     * @param modulo
     * @return
     */
    private int incrementAndGetModulo( AtomicInteger cyclicCounter , int modulo) {
        for (;;) {
            int current = cyclicCounter.get();
            int next = (current + 1) % modulo;
            if (cyclicCounter.compareAndSet(current, next)){
                return next;
            }
        }
    }


}
