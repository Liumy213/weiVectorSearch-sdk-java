package io.github.liumy213.client;

import io.github.liumy213.rpc.*;

public interface VectorSearchBrpc{
    HasCollectionResponse has_collection(HasCollectionRequest hasCollectionRequest);
    Status create_collection(CreateCollectionRequest createCollectionRequest);
    Status drop_collection(DropCollectionRequest dropCollectionRequest);
    DescribeCollectionResponse describe_collection(DescribeCollectionRequest describeCollectionRequest);

    Status create_index(CreateIndexRequest createIndexRequest);
    Status drop_index(DropIndexRequest dropIndexRequest);

    InsertResponse insert_entity(InsertRequest insertRequest);
    SearchResponse search_entity(SearchRequest searchRequest);
}
