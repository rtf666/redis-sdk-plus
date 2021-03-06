package com.rtf.redis.client.lb;

import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * redis断路器
 * @Author : liupeng
 * @Date : 2020-02-12
 * @Modified By
 */
@Setter
@Getter
@Slf4j
public class AppRedisCircuitBreaker {

    private static Map<String,AppRedisCircuitBreaker> redisCircuitBreakers = Maps.newConcurrentMap() ;

    /**
     * 连续失败次数的阀值
     */
    private final int successiveFailureThreshold = 5 ;

    /**
     * 断路器超时时长，单位毫秒。默认120秒
     */
    private final int circuitTrippedTimeout = 120 * 1000 ;

    /**
     * 上一次断路器打开的时间
     */
    private volatile long lastCircuitBreakerTrippedTimestamp = 0 ;

    /**
     * 连续失败次数
     */
    private AtomicInteger successiveFailureCount = new AtomicInteger(0) ;

    /**
     * redis host 配置
     */
    private volatile String host ;

    /**
     * 获取断路器示例
     * @param host
     * @return
     */
    public static AppRedisCircuitBreaker getInstance(String host){
        if( redisCircuitBreakers.containsKey( host )  ){
            return redisCircuitBreakers.get( host ) ;
        }
        // 创建断路器
        AppRedisCircuitBreaker appRedisCircuitBreaker = new AppRedisCircuitBreaker(host) ;
        redisCircuitBreakers.putIfAbsent( host , appRedisCircuitBreaker ) ;

        return redisCircuitBreakers.get( host ) ;
    }

    /**
     * 移除断路器
     * @param host
     */
    public static void remove(String host){
        redisCircuitBreakers.remove( host ) ;
    }

    /**
     * 获取所有的断路器实例
     * @return
     */
    public static Collection<AppRedisCircuitBreaker> getInstances(){
        return redisCircuitBreakers.values() ;
    }

    private AppRedisCircuitBreaker(String host){
        this.host = host ;
    }

    /**
     * 短路器是否打开
     * @return
     */
    public boolean isCircuitBreakerTripped() {
        long circuitBreakerTimeout = getCircuitBreakerTimeout();
        if (circuitBreakerTimeout <= 0) {
            return false;
        }
        return circuitBreakerTimeout >= System.currentTimeMillis() ;
    }

    /**
     * 获取断路器失效的时间点
     * @return
     */
    private long getCircuitBreakerTimeout() {
        long blackOutPeriod = getCircuitBreakerBlackoutPeriod();
        if (blackOutPeriod <= 0) {
            return 0;
        }
        return lastCircuitBreakerTrippedTimestamp + blackOutPeriod;
    }

    private long getCircuitBreakerBlackoutPeriod() {
        int failureCount = successiveFailureCount.get();
        if (failureCount < successiveFailureThreshold) {
            return 0;
        }
        // 使用固定的断路器时间间隔
        return circuitTrippedTimeout ;
    }

    /**
     * 增加断路器失败次数
     */
    public void incrementSuccessiveFailureCount() {
        // 连续失败次数超过阀值，并且处于正常状态
        if( successiveFailureCount.get() > successiveFailureThreshold ){
            if( !isCircuitBreakerTripped() ){
                lastCircuitBreakerTrippedTimestamp = System.currentTimeMillis() ;
                log.error("redis主机:{} 连续失败次数为:{},触发熔断,熔断结束时间:{}" , host , successiveFailureCount.get() ,
                        new DateTime(lastCircuitBreakerTrippedTimestamp).toString("yyyy-MM-dd HH:mm:ss")) ;
            }
        }else{
            successiveFailureCount.incrementAndGet() ;
        }
    }

    /**
     * 清除断路器失败次数
     */
    public void clearSuccessiveFailureCount() {
        // 处于半熔断状态，则允许清除连续失败次数
        if( !isCircuitBreakerTripped() ){
            successiveFailureCount.set(0) ;
        }else{
            log.info("redis主机:{} 处于熔断状态" , host );
        }
    }

}
