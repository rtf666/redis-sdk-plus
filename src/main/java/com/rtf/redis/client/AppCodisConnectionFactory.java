package com.rtf.redis.client;

import com.rtf.redis.client.lb.AppRedisCircuitBreaker;
import com.rtf.redis.client.lb.AppRedisHealthStats;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

/**
 * redis连接工厂
 * @Author : liupeng
 * @Date : 2019-03-27
 * @Modified By
 */
@Slf4j
public class AppCodisConnectionFactory extends LettuceConnectionFactory {

    private AppRedisHealthStats appRedisHealthStats ;

    @Setter
    @Getter
    private boolean enableHealthCheck = false ;

    @Setter
    @Getter
    private boolean master = true ;

    public AppCodisConnectionFactory(RedisStandaloneConfiguration standaloneConfig,
                                     LettuceClientConfiguration clientConfig) {
        super( standaloneConfig , clientConfig ) ;
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        // 开启健康检查
        if( enableHealthCheck ){
            appRedisHealthStats = new AppRedisHealthStats( this ) ;
            appRedisHealthStats.start();
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        // 销毁统计信息
        if( enableHealthCheck && appRedisHealthStats!=null ){
            // 移除统计信息
            appRedisHealthStats.destory() ;
            // 移除断路器
            AppRedisCircuitBreaker.remove( getHostName() ) ;
        }
    }

    public AppCodisConnectionFactory(RedisSentinelConfiguration sentinelConfiguration,
                                     LettuceClientConfiguration clientConfig) {
        throw new RuntimeException("not support RedisSentinelConfiguration") ;
    }

    public AppCodisConnectionFactory(RedisClusterConfiguration clusterConfiguration,
                                     LettuceClientConfiguration clientConfig) {
        throw new RuntimeException("not support RedisClusterConfiguration") ;
    }

    @Override
    public RedisConnection getConnection() {
        RedisConnection redisConnection = super.getConnection() ;

        return createRedisConnectionProxy( redisConnection ) ;
    }

    /**
     * 创建redis连接
     * @param targetRedisConnection
     * @return
     */
    public RedisConnection createRedisConnectionProxy(RedisConnection targetRedisConnection){
        // 如果已经是aop代理，则不再重新搭建
        if( AopUtils.isAopProxy( targetRedisConnection ) ){
            log.info("RedisConnection已经是代理对象");
            return targetRedisConnection ;
        }

        ProxyFactory proxyFactory = new ProxyFactory() ;
        proxyFactory.setTarget( targetRedisConnection ) ;
        proxyFactory.setInterfaces( RedisConnection.class ) ;
        proxyFactory.setProxyTargetClass( true ) ;
        // 设置目标拦截器
        proxyFactory.addAdvice( new AppRedisConnectionMethodInterceptor( getHostName() ) ) ;

        return (RedisConnection)proxyFactory.getProxy() ;
    }
}
