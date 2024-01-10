package io.github.liumy213.client;

import com.baidu.cloud.starlight.api.rpc.StarlightClient;
import com.baidu.cloud.starlight.api.rpc.config.ServiceConfig;
import com.baidu.cloud.starlight.api.rpc.config.TransportConfig;
import com.baidu.cloud.starlight.core.rpc.SingleStarlightClient;
import com.baidu.cloud.starlight.core.rpc.proxy.JDKProxyFactory;

public class BrpcClientConfig {
    public BrpcClientConfig() {}
    public BrpcClientConfig(String brpcHost, int brpcPort) {
        config = new TransportConfig();
        starlightClient = new SingleStarlightClient(brpcHost, brpcPort, config);
        starlightClient.init();

        // 服务配置
        clientConfig = new ServiceConfig();
        clientConfig.setProtocol("brpc");
        clientConfig.setServiceId("VectorSearchService");

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

    public void releaseClient() {
        starlightClient.destroy();
    }

}
