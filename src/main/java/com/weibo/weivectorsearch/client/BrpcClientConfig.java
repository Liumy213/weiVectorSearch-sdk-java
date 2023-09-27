package com.weibo.weivectorsearch.client;

import com.baidu.cloud.starlight.api.rpc.StarlightClient;
import com.baidu.cloud.starlight.api.rpc.config.ServiceConfig;
import com.baidu.cloud.starlight.api.rpc.config.TransportConfig;
import com.baidu.cloud.starlight.core.rpc.SingleStarlightClient;
import com.baidu.cloud.starlight.core.rpc.proxy.JDKProxyFactory;

public class BrpcClientConfig {
    public BrpcClientConfig() {}
    public BrpcClientConfig(String brpcHost, int brpcPort) {
        config = new TransportConfig(); // 传输配置
        starlightClient = new SingleStarlightClient(brpcHost, brpcPort, config);
        starlightClient.init();

        // 服务配置
        clientConfig = new ServiceConfig(); // 服务配置
        clientConfig.setProtocol("brpc");
        clientConfig.setServiceId("VectorSearchService"); // 跨语言时指定服务端定义的serviceName

        proxyFactory = new JDKProxyFactory();
    }

    private StarlightClient starlightClient;
    private TransportConfig config;
    private ServiceConfig clientConfig;
    private JDKProxyFactory proxyFactory;
    public VectorSearchBrpc getVectorProto () {
        // 生成代理
        VectorSearchBrpc userService = proxyFactory.getProxy(VectorSearchBrpc.class, clientConfig, starlightClient);
        return userService;
    }

}
