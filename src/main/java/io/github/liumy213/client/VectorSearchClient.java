package io.github.liumy213.client;

import io.github.liumy213.param.dml.*;
import io.github.liumy213.param.FlushParam;
import io.github.liumy213.param.R;
import io.github.liumy213.param.RpcStatus;
import io.github.liumy213.param.collection.*;
import io.github.liumy213.param.collection.*;
import io.github.liumy213.param.control.GetFlushStateParam;
import io.github.liumy213.param.dml.DeleteParam;
import io.github.liumy213.param.dml.InsertParam;
import io.github.liumy213.param.dml.QueryParam;
import io.github.liumy213.param.dml.SearchParam;
import io.github.liumy213.param.index.*;
import io.github.liumy213.param.partition.*;
import io.github.liumy213.rpc.*;
import io.github.liumy213.param.index.*;
import io.github.liumy213.param.partition.*;
import io.github.liumy213.rpc.*;

public interface VectorSearchClient {
    R<Boolean> hasCollection(HasCollectionParam requestParam);
    R<RpcStatus> createCollection(CreateCollectionParam requestParam);
    R<RpcStatus> dropCollection(DropCollectionParam requestParam);
    R<RpcStatus> loadCollection(LoadCollectionParam requestParam);
    R<RpcStatus> releaseCollection(ReleaseCollectionParam requestParam);
    R<ShowCollectionsResponse> showCollections(ShowCollectionsParam requestParam);
    R<DescribeCollectionResponse> describeCollection(DescribeCollectionParam requestParam);

    R<RpcStatus> createPartition(CreatePartitionParam requestParam);
    R<RpcStatus> dropPartition(DropPartitionParam requestParam);
    R<Boolean> hasPartition(HasPartitionParam requestParam);
    R<RpcStatus> loadPartitions(LoadPartitionsParam requestParam);
    R<RpcStatus> releasePartitions(ReleasePartitionsParam requestParam);
    R<ShowPartitionsResponse> showPartitions(ShowPartitionsParam requestParam);

    R<RpcStatus> createIndex(CreateIndexParam requestParam);
    R<RpcStatus> dropIndex(DropIndexParam requestParam);
    R<DescribeIndexResponse> describeIndex(DescribeIndexParam requestParam);
    R<GetIndexStateResponse> getIndexState(GetIndexStateParam requestParam);
    R<GetIndexBuildProgressResponse> getIndexBuildProgress(GetIndexBuildProgressParam requestParam);

    R<MutationResult> insert(InsertParam requestParam);
    R<SearchResults> search(SearchParam requestParam);
    R<QueryResults> query(QueryParam requestParam);
    R<FlushResponse> flush(FlushParam requestParam);
    R<MutationResult> delete(DeleteParam requestParam);

    R<GetFlushStateResponse> getFlushState(GetFlushStateParam requestParam);
}
