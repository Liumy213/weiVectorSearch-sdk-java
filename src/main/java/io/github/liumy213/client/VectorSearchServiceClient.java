package io.github.liumy213.client;

import io.github.liumy213.param.ConnectParam;
import io.github.liumy213.param.R;
import io.github.liumy213.param.RpcStatus;
import io.github.liumy213.param.collection.*;
import io.github.liumy213.param.dml.DeleteParam;
import io.github.liumy213.param.dml.InsertParam;
import io.github.liumy213.param.dml.QueryParam;
import io.github.liumy213.param.dml.SearchParam;
import io.github.liumy213.param.index.CreateIndexParam;
import io.github.liumy213.param.index.DescribeIndexParam;
import io.github.liumy213.param.index.DropIndexParam;
import io.github.liumy213.param.partition.*;
import io.github.liumy213.rpc.*;
import lombok.NonNull;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class VectorSearchServiceClient extends AbstractVectorSearchBrpcClient {
    private final BrpcClientConfig brpcClientConfig;
    private final VectorSearchBrpc vectorSearchBrpc;
    private long timeoutMs = 0;
    private int retryTimes = 0;
    private long retryIntervalMs = 500L;

    public VectorSearchServiceClient(@NonNull ConnectParam connectParam) {
        String host = connectParam.getHost();
        int port = connectParam.getPort();
        this.brpcClientConfig = new BrpcClientConfig(host, port);
        this.vectorSearchBrpc = brpcClientConfig.getVectorProto();
    }

    private <T> R<T> retry(Callable<R<T>> callable) {
        // no retry, direct call the method
        if (this.retryTimes <= 1) {
            try {
                return callable.call();
            } catch (Exception e) {
                return R.failed(e);
            }
        }

        // method to check timeout
        long begin = System.currentTimeMillis();
        Callable<Void> timeoutChecker = ()->{
            long current = System.currentTimeMillis();
            long cost = (current - begin);
            if (this.timeoutMs > 0 && cost >= this.timeoutMs) {
                String msg = String.format("Retry timeout: %dms", this.timeoutMs);
                throw new RuntimeException(msg);
            }
            return null;
        };

        // retry within timeout
        for (int i = 0; i < this.retryTimes; i++) {
            try {
                R<T> resp = callable.call();
                if (resp.getStatus() == R.Status.Success.getCode()) {
                    return resp;
                }

                if (i != this.retryTimes-1) {
                    timeoutChecker.call();
                    TimeUnit.MILLISECONDS.sleep(this.retryIntervalMs);
                    timeoutChecker.call();
                    logInfo(String.format("Retry again after %dms...", this.retryIntervalMs));
                }
            } catch (Exception e) {
                logError(e.getMessage());
                return R.failed(e);
            }
        }
        String msg = String.format("Retry run out of %d retry times", this.retryTimes);
        logError(msg);
        return R.failed(new RuntimeException(msg));
    }

    @Override
    protected VectorSearchBrpc vectorSearchBrpc() {
        return this.vectorSearchBrpc;
    }

    @Override
    public R<Boolean> hasCollection(HasCollectionParam hasCollectionParam) {
        return retry(()-> super.hasCollection(hasCollectionParam));
    }

    @Override
    public R<RpcStatus> createCollection(CreateCollectionParam createCollectionParam) {
        return retry(()-> super.createCollection(createCollectionParam));
    }

    @Override
    public R<RpcStatus> dropCollection(DropCollectionParam requestParam) {
        return retry(()-> super.dropCollection(requestParam));
    }

    @Override
    public R<ShowCollectionsResponse> showCollections(ShowCollectionsParam requestParam) {
        return retry(()-> super.showCollections(requestParam));
    }

    @Override
    public R<DescribeCollectionResponse> describeCollection(DescribeCollectionParam requestParam) {
        return retry(()-> super.describeCollection(requestParam));
    }

    @Override
    public R<RpcStatus> createPartition(CreatePartitionParam requestParam) {
        return retry(()-> super.createPartition(requestParam));
    }

    @Override
    public R<RpcStatus> dropPartition(DropPartitionParam requestParam) {
        return retry(()-> super.dropPartition(requestParam));
    }

    @Override
    public R<Boolean> hasPartition(HasPartitionParam requestParam) {
        return retry(()-> super.hasPartition(requestParam));
    }

    @Override
    public R<ShowPartitionsResponse> showPartitions(ShowPartitionsParam requestParam) {
        return retry(()-> super.showPartitions(requestParam));
    }

    @Override
    public R<RpcStatus> createIndex(CreateIndexParam requestParam) {
        return retry(()-> super.createIndex(requestParam));
    }

    @Override
    public R<RpcStatus> dropIndex(DropIndexParam requestParam) {
        return retry(()-> super.dropIndex(requestParam));
    }

    @Override
    public R<DescribeIndexResponse> describeIndex(DescribeIndexParam requestParam) {
        return retry(()-> super.describeIndex(requestParam));
    }

    @Override
    public R<MutationResult> insert(InsertParam requestParam) {
        return retry(()-> super.insert(requestParam));
    }

    @Override
    public R<SearchResults> search(SearchParam requestParam) {
        return retry(()-> super.search(requestParam));
    }

    @Override
    public R<QueryResults> query(QueryParam requestParam) {
        return retry(()-> super.query(requestParam));
    }

    @Override
    public R<MutationResult> delete(DeleteParam requestParam) {
        return retry(()-> super.delete(requestParam));
    }
}
