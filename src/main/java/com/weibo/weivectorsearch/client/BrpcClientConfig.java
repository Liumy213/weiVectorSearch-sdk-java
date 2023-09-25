package com.weibo.weivectorsearch.client;

import com.baidu.cloud.starlight.api.rpc.StarlightClient;
import com.baidu.cloud.starlight.api.rpc.config.ServiceConfig;
import com.baidu.cloud.starlight.api.rpc.config.TransportConfig;
import com.baidu.cloud.starlight.core.rpc.SingleStarlightClient;
import com.baidu.cloud.starlight.core.rpc.proxy.JDKProxyFactory;
import com.weibo.weivectorsearch.rpc.*;

public class BrpcClientConfig {
    public BrpcClientConfig() {}
    public BrpcClientConfig(String brpcHost, int brpcPort) {
        TransportConfig config = new TransportConfig(); // 传输配置
        StarlightClient starlightClient = new SingleStarlightClient(brpcHost, brpcPort, config);
        starlightClient.init();

        // 服务配置
        ServiceConfig clientConfig = new ServiceConfig(); // 服务配置
        clientConfig.setProtocol("brpc");
        clientConfig.setServiceId("RpcService"); // 跨语言时指定服务端定义的serviceName

        JDKProxyFactory proxyFactory = new JDKProxyFactory();
    }

//    public

    // 生成代理
//    VectorSearchService userService = proxyFactory.getProxy(UserProtoService.class, clientConfig, starlightClient);

}
