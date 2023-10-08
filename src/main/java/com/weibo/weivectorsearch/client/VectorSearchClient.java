package com.weibo.weivectorsearch.client;

import com.weibo.weivectorsearch.param.dml.*;
import com.weibo.weivectorsearch.param.FlushParam;
import com.weibo.weivectorsearch.param.R;
import com.weibo.weivectorsearch.param.RpcStatus;
import com.weibo.weivectorsearch.param.collection.*;
import com.weibo.weivectorsearch.param.control.GetFlushStateParam;
import com.weibo.weivectorsearch.param.dml.DeleteParam;
import com.weibo.weivectorsearch.param.dml.InsertParam;
import com.weibo.weivectorsearch.param.dml.QueryParam;
import com.weibo.weivectorsearch.param.dml.SearchParam;
import com.weibo.weivectorsearch.param.index.*;
import com.weibo.weivectorsearch.param.partition.*;
import com.weibo.weivectorsearch.rpc.*;

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
