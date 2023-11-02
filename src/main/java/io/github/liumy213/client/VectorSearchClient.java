package io.github.liumy213.client;

import io.github.liumy213.param.R;
import io.github.liumy213.param.RpcStatus;
import io.github.liumy213.param.collection.*;
import io.github.liumy213.param.dml.InsertParam;
import io.github.liumy213.param.dml.QueryParam;
import io.github.liumy213.param.dml.SearchParam;
import io.github.liumy213.param.index.CreateIndexParam;
import io.github.liumy213.param.index.DropIndexParam;
import io.github.liumy213.param.partition.*;
import io.github.liumy213.rpc.*;

public interface VectorSearchClient {
    R<Boolean> hasCollection(HasCollectionParam requestParam);
    R<RpcStatus> createCollection(CreateCollectionParam requestParam);
    R<RpcStatus> dropCollection(DropCollectionParam requestParam);
    R<DescribeCollectionResponse> describeCollection(DescribeCollectionParam requestParam);

    R<RpcStatus> createPartition(CreatePartitionParam requestParam);
    R<RpcStatus> dropPartition(DropPartitionParam requestParam);
    R<Boolean> hasPartition(HasPartitionParam requestParam);

    R<RpcStatus> createIndex(CreateIndexParam requestParam);
    R<RpcStatus> dropIndex(DropIndexParam requestParam);

    R<InsertResponse> insert(InsertParam requestParam);
    R<SearchResponse> search(SearchParam requestParam);
    R<QueryResponse> query(QueryParam requestParam);
}
