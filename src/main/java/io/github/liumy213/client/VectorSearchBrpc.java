package io.github.liumy213.client;

import io.github.liumy213.rpc.*;

public interface VectorSearchBrpc{
    BoolResponse has_collection(HasCollectionRequest hasCollectionRequest);
    Status create_collection(CreateCollectionRequest createCollectionRequest);
    Status drop_collection(DropCollectionRequest dropCollectionRequest);
    DescribeCollectionResponse describe_collection(DescribeCollectionRequest describeCollectionRequest);

    Status create_partition(CreatePartitionRequest createPartitionRequest);
    Status drop_partition(DropPartitionRequest dropPartitionRequest);
    BoolResponse has_partition(HasPartitionRequest hasPartitionRequest);

    Status create_index(CreateIndexRequest createIndexRequest);
    Status drop_index(DropIndexRequest dropIndexRequest);

    MutationResult insert_entity(InsertRequest insertRequest);
    SearchResults search_entity(SearchRequest searchRequest);
    QueryResults query_entity(QueryRequest queryRequest);
}
