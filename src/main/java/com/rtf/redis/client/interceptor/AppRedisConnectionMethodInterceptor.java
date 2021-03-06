package com.rtf.redis.client.interceptor;

import com.rtf.redis.client.lb.AppRedisCircuitBreaker;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.reflections.ReflectionUtils;
import org.springframework.data.redis.connection.RedisCommands;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * redis连接拦截器
 * @Author : liupeng
 * @Date : 2019-03-27
 * @Modified By
 */
@Slf4j
public class AppRedisConnectionMethodInterceptor implements MethodInterceptor {

    @Setter
    @Getter
    private String host ;

    private static Set<String> REDIS_CMD_METHODS = Sets.newHashSet() ;

    static {
        // redis的所有命令
        REDIS_CMD_METHODS = ReflectionUtils.getAllMethods(RedisCommands.class)
                .stream().map( method -> method.getName() ).collect(Collectors.toSet()) ;
        log.debug("redis拦截命令: {}" , Joiner.on(",").join( REDIS_CMD_METHODS )); ;
    }

    public AppRedisConnectionMethodInterceptor(String host){
        this.host = host ;
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        // 1. 获取熔断对象实例
        AppRedisCircuitBreaker appRedisCircuitBreaker = AppRedisCircuitBreaker.getInstance( host ) ;

        Object redisResult = null ;

        try{
            redisResult = invocation.proceed() ;

            // 只对redis的执行的命令进行拦截，不拦截一般逻辑方法
            if( REDIS_CMD_METHODS.contains( invocation.getMethod().getName() ) ){
                appRedisCircuitBreaker.clearSuccessiveFailureCount() ;
            }

        }catch( Exception e ){
            // 只对redis的执行的命令进行拦截，增加连续失败的次数
            if( REDIS_CMD_METHODS.contains( invocation.getMethod().getName() ) ){
                log.error( "redis主机:{} 中命令 {} 执行异常, {}" , host , invocation.getMethod().getName() , e.getMessage()) ;
                appRedisCircuitBreaker.incrementSuccessiveFailureCount() ;
            }

            throw e ;
        }

        return redisResult ;
    }

}
