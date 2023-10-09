package io.github.liumy213.client;

import io.github.liumy213.rpc.*;
import io.github.liumy213.rpc.*;

public interface VectorSearchBrpc{
    BoolResponse has_collection(HasCollectionRequest hasCollectionRequest);
    Status create_collection(CreateCollectionRequest createCollectionRequest);
    Status drop_collection(DropCollectionRequest dropCollectionRequest);
    Status load_collection(LoadCollectionRequest loadCollectionRequest);
    Status release_collection(ReleaseCollectionRequest releaseCollectionRequest);
    ShowCollectionsResponse show_collections(ShowCollectionsRequest showCollectionsRequest);
    DescribeCollectionResponse describe_collection(DescribeCollectionRequest describeCollectionRequest);

    Status create_partition(CreatePartitionRequest createPartitionRequest);
    Status drop_partition(DropPartitionRequest dropPartitionRequest);
    BoolResponse has_partition(HasPartitionRequest hasPartitionRequest);
    Status load_partitions(LoadPartitionsRequest loadPartitionsRequest);
    Status release_partitions(ReleasePartitionsRequest releasePartitionsRequest);
    ShowPartitionsResponse show_partitions(ShowPartitionsRequest showPartitionsRequest);

    Status create_index(CreateIndexRequest createIndexRequest);
    DescribeIndexResponse describe_index(DescribeIndexRequest describeIndexRequest);
    Status drop_index(DropIndexRequest dropIndexRequest);
    GetIndexStateResponse get_index_state(GetIndexStateRequest getIndexStateRequest);
    GetIndexBuildProgressResponse get_index_build_progress(GetIndexBuildProgressRequest getIndexBuildProgressRequest);

    MutationResult insert(InsertRequest insertRequest);
    MutationResult delete(DeleteRequest deleteRequest);
    SearchResults search(SearchRequest searchRequest);
    FlushResponse flush(FlushRequest flushRequest);
    QueryResults query(QueryRequest queryRequest);

    GetFlushStateResponse get_flush_state(GetFlushStateRequest getFlushStateRequest);
}
