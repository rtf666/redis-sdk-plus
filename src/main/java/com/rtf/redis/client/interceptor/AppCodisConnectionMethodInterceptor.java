package com.rtf.redis.client.interceptor;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.aopalliance.intercept.MethodInvocation;

import java.util.Set;

/**
 * codis连接拦截器
 * @Author : liupeng
 * @Date : 2019-03-27
 * @Modified By
 */
@Slf4j
public class AppCodisConnectionMethodInterceptor extends AppRedisConnectionMethodInterceptor {

    /**
     * 不支持的redis方法
     */
    public static final Set<String> UN_SUPPORT_METHODS = Sets.newHashSet(
            "setConfig" , "getConfig" , "configGet" ,
            "rename" , "renameNX" ,
            "bLPop" , "bRPop" , "bRPopLPush" , "getSubscription" ,
            "publish" , "subscribe" , "pSubscribe" ) ;

    public AppCodisConnectionMethodInterceptor(String host){
        super(host);
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        // 过滤不支持的redis方法
        if( UN_SUPPORT_METHODS.contains( invocation.getMethod().getName() ) ){
            return null ;
        }

        return super.invoke( invocation ) ;
    }

}
