package io.github.liumy213.client;

import com.baidu.fengchao.stargate.remoting.exceptions.RpcExecutionException;
import io.github.liumy213.param.*;
import io.github.liumy213.param.collection.*;
import io.github.liumy213.param.*;
import io.github.liumy213.param.collection.*;
import io.github.liumy213.param.control.GetFlushStateParam;
import io.github.liumy213.param.dml.DeleteParam;
import io.github.liumy213.param.dml.InsertParam;
import io.github.liumy213.param.dml.QueryParam;
import io.github.liumy213.param.dml.SearchParam;
import io.github.liumy213.exception.IllegalResponseException;
import io.github.liumy213.exception.ParamException;
import io.github.liumy213.param.index.*;
import io.github.liumy213.param.partition.*;
import io.github.liumy213.param.partition.*;
import io.github.liumy213.response.DescCollResponseWrapper;
import io.github.liumy213.rpc.*;
import io.github.liumy213.param.index.*;
import io.github.liumy213.rpc.*;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class AbstractVectorSearchBrpcClient implements VectorSearchClient {
    protected static final Logger logger = LoggerFactory.getLogger(AbstractVectorSearchBrpcClient.class);
    protected LogLevel logLevel = LogLevel.Info;
    protected abstract VectorSearchBrpc vectorSearchBrpc();

    private <T> R<T> failedStatus(String requestName, Status status) {
        String reason = status.getReason();
        if (StringUtils.isEmpty(reason)) {
            reason = "error code: " + status.getErrorCode().toString();
        }
        logError(requestName + " failed:{}", reason);
        return R.failed(R.Status.valueOf(status.getErrorCode().getNumber()), reason);
    }

    @Override
    public R<Boolean> hasCollection(@NonNull HasCollectionParam requestParam) {
        logInfo(requestParam.toString());

        try {
            HasCollectionRequest.Builder builder = HasCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            HasCollectionRequest hasCollectionRequest = builder
                    .build();

            BoolResponse response = vectorSearchBrpc().has_collection(hasCollectionRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("HasCollectionRequest successfully!");
                Boolean value = Optional.of(response)
                        .map(BoolResponse::getValue)
                        .orElse(false);
                return R.success(value);
            } else {
                return failedStatus("HasCollectionRequest", response.getStatus());
            }
        } catch (RpcExecutionException e) {
            logError("HasCollectionRequest RPC failed:{}", requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("HasCollectionRequest failed:{}", requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createCollection(@NonNull CreateCollectionParam requestParam) {
        logInfo(requestParam.toString());

        try {
            // Construct CollectionSchema Params
            CollectionSchema.Builder collectionSchemaBuilder = CollectionSchema.newBuilder();
            collectionSchemaBuilder.setName(requestParam.getCollectionName())
                    .setDescription(requestParam.getDescription());

            for (FieldType fieldType : requestParam.getFieldTypes()) {
                collectionSchemaBuilder.addFields(ParamUtils.ConvertField(fieldType));
            }

            // Construct CreateCollectionRequest
            CreateCollectionRequest.Builder builder = CreateCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setShardsNum(requestParam.getShardsNum())
                    .setSchema(collectionSchemaBuilder.build().toBuilder());
            if (requestParam.getPartitionsNum() > 0) {
                builder.setNumPartitions(requestParam.getPartitionsNum());
            }

            CreateCollectionRequest createCollectionRequest = builder.build();

            Status response = vectorSearchBrpc().create_collection(createCollectionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("CreateCollectionRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("CreateCollectionRequest", response);
            }
        } catch (RpcExecutionException e) {
            logError("CreateCollectionRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateCollectionRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropCollection(@NonNull DropCollectionParam requestParam) {
        logInfo(requestParam.toString());

        try {
            DropCollectionRequest.Builder builder = DropCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName());
            DropCollectionRequest dropCollectionRequest = builder.build();

            Status response = vectorSearchBrpc().drop_collection(dropCollectionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("DropCollectionRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("DropCollectionRequest", response);
            }
        } catch (RpcExecutionException e) {
            logError("DropCollectionRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DropCollectionRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    private void waitForLoadingCollection(String databaseName, String collectionName, List<String> partitionNames,
                                          long waitingInterval, long timeout) throws IllegalResponseException {
        long tsBegin = System.currentTimeMillis();
        if (partitionNames == null || partitionNames.isEmpty()) {
            ShowCollectionsRequest.Builder builder = ShowCollectionsRequest.newBuilder()
                    .addCollectionNames(collectionName)
                    .setType(ShowType.InMemory);
            ShowCollectionsRequest showCollectionRequest = builder.build();

            // Use showCollection() to check loading percentages of the collection.
            // If the inMemory percentage is 100, that means the collection has finished loading.
            // Otherwise, this thread will sleep a small interval and check again.
            // If waiting time exceed timeout, exist the circle
            while (true) {
                long tsNow = System.currentTimeMillis();
                if ((tsNow - tsBegin) >= timeout * 1000) {
                    logWarning("Waiting load thread is timeout, loading process may not be finished");
                    break;
                }

                ShowCollectionsResponse response = vectorSearchBrpc().show_collections(showCollectionRequest);
                int namesCount = response.getCollectionNamesCount();
                int percentagesCount = response.getInMemoryPercentagesCount();
                if (namesCount != 1) {
                    throw new IllegalResponseException("ShowCollectionsResponse is illegal. Collection count: "
                            + namesCount);
                }

                if (namesCount != percentagesCount) {
                    String msg = "ShowCollectionsResponse is illegal. Collection count: " + namesCount
                            + " memory percentages count: " + percentagesCount;
                    throw new IllegalResponseException(msg);
                }

                long percentage = response.getInMemoryPercentages(0);
                String responseCollection = response.getCollectionNames(0);
                if (responseCollection.compareTo(collectionName) == 0 && percentage >= 100) {
                    break;
                }

                try {
                    logDebug("Waiting load, interval: {} ms, percentage: {}%", waitingInterval, percentage);
                    TimeUnit.MILLISECONDS.sleep(waitingInterval);
                } catch (InterruptedException e) {
                    logWarning("Waiting load thread is interrupted, loading process may not be finished");
                    break;
                }
            }

        } else {
            ShowPartitionsRequest.Builder builder = ShowPartitionsRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .addAllPartitionNames(partitionNames);
            if (StringUtils.isNotEmpty(databaseName)) {
                builder.setDbName(databaseName);
            }
            ShowPartitionsRequest showPartitionsRequest = builder.setType(ShowType.InMemory).build();

            // Use showPartitions() to check loading percentages of all the partitions.
            // If each partition's  inMemory percentage is 100, that means all the partitions have finished loading.
            // Otherwise, this thread will sleep a small interval and check again.
            // If waiting time exceed timeout, exist the circle
            while (true) {
                long tsNow = System.currentTimeMillis();
                if ((tsNow - tsBegin) >= timeout * 1000) {
                    logWarning("Waiting load thread is timeout, loading process may not be finished");
                    break;
                }

                ShowPartitionsResponse response = vectorSearchBrpc().show_partitions(showPartitionsRequest);
                int namesCount = response.getPartitionNamesCount();
                int percentagesCount = response.getInMemoryPercentagesCount();
                if (namesCount != percentagesCount) {
                    String msg = "ShowPartitionsResponse is illegal. Partition count: " + namesCount
                            + " memory percentages count: " + percentagesCount;
                    throw new IllegalResponseException(msg);
                }

                // construct a hash map to check each partition's inMemory percentage by name
                Map<String, Long> percentages = new HashMap<>();
                for (int i = 0; i < response.getInMemoryPercentagesCount(); ++i) {
                    percentages.put(response.getPartitionNames(i), response.getInMemoryPercentages(i));
                }

                String partitionNoMemState = "";
                String partitionNotFullyLoad = "";
                boolean allLoaded = true;
                for (String name : partitionNames) {
                    if (!percentages.containsKey(name)) {
                        allLoaded = false;
                        partitionNoMemState = name;
                        break;
                    }
                    if (percentages.get(name) < 100L) {
                        allLoaded = false;
                        partitionNotFullyLoad = name;
                        break;
                    }
                }

                if (allLoaded) {
                    break;
                }

                try {
                    String msg = "Waiting load, interval: " + waitingInterval + "ms";
                    if (!partitionNoMemState.isEmpty()) {
                        msg += ("Partition " + partitionNoMemState + " has no memory state");
                    }
                    if (!partitionNotFullyLoad.isEmpty()) {
                        msg += ("Partition " + partitionNotFullyLoad + " has not fully loaded");
                    }
                    logDebug(msg);
                    TimeUnit.MILLISECONDS.sleep(waitingInterval);
                } catch (InterruptedException e) {
                    logWarning("Waiting load thread is interrupted, load process may not be finished");
                    break;
                }
            }
        }
    }


    @Override
    public R<RpcStatus> loadCollection(@NonNull LoadCollectionParam requestParam) {
        logInfo(requestParam.toString());

        try {
            LoadCollectionRequest.Builder builder = LoadCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName());
            LoadCollectionRequest loadCollectionRequest = builder
                    .build();

            Status response = vectorSearchBrpc().load_collection(loadCollectionRequest);

            if (response.getErrorCode() != ErrorCode.Success) {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }

//             sync load, wait until collection finish loading
            if (requestParam.isSyncLoad()) {
                waitForLoadingCollection(requestParam.getDatabaseName(), requestParam.getCollectionName(), null,
                        requestParam.getSyncLoadWaitingInterval(), requestParam.getSyncLoadWaitingTimeout());
            }

            logDebug("LoadCollectionRequest successfully! Collection name:{}",
                    requestParam.getCollectionName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (RpcExecutionException e) { // gRPC could throw this exception
            logError("LoadCollectionRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) { // milvus exception for illegal response
            logError("LoadCollectionRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> releaseCollection(@NonNull ReleaseCollectionParam requestParam) {
        logInfo(requestParam.toString());

        try {
            ReleaseCollectionRequest releaseCollectionRequest = ReleaseCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .build();

            Status response = vectorSearchBrpc().release_collection(releaseCollectionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("ReleaseCollectionRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("ReleaseCollectionRequest", response);
            }
        } catch (RpcExecutionException e) {
            logError("ReleaseCollectionRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("ReleaseCollectionRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }


    @Override
    public R<ShowCollectionsResponse> showCollections(@NonNull ShowCollectionsParam requestParam) {
        logInfo(requestParam.toString());

        try {
            ShowCollectionsRequest.Builder builder = ShowCollectionsRequest.newBuilder()
                    .addAllCollectionNames(requestParam.getCollectionNames())
                    .setType(requestParam.getShowType());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            ShowCollectionsRequest showCollectionsRequest = builder.build();

            ShowCollectionsResponse response = vectorSearchBrpc().show_collections(showCollectionsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("ShowCollectionsRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("ShowCollectionsRequest", response.getStatus());
            }
        } catch (RpcExecutionException e) {
            logError("ShowCollectionsRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("ShowCollectionsRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<DescribeCollectionResponse> describeCollection(@NonNull DescribeCollectionParam requestParam) {
        logInfo(requestParam.toString());

        try {
            DescribeCollectionRequest.Builder builder = DescribeCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName());
            DescribeCollectionRequest describeCollectionRequest = builder.build();

            DescribeCollectionResponse response = vectorSearchBrpc().describe_collection(describeCollectionRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("DescribeCollectionRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("DescribeCollectionRequest", response.getStatus());
            }
        } catch (RpcExecutionException e) {
            logError("DescribeCollectionRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DescribeCollectionRequest failed!", e);
            return R.failed(e);
        }
    }


    @Override
    public R<RpcStatus> createPartition(@NonNull CreatePartitionParam requestParam) {
        logInfo(requestParam.toString());

        try {
            CreatePartitionRequest createPartitionRequest = CreatePartitionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName())
                    .build();

            Status response = vectorSearchBrpc().create_partition(createPartitionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("CreatePartitionRequest successfully! Collection name:{}, partition name:{}",
                        requestParam.getCollectionName(), requestParam.getPartitionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("CreatePartitionRequest", response);
            }
        } catch (RpcExecutionException e) {
            logError("CreatePartitionRequest RPC failed! Collection name:{}, partition name:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("CreatePartitionRequest failed! Collection name:{}, partition name:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropPartition(@NonNull DropPartitionParam requestParam) {
        logInfo(requestParam.toString());

        try {
            DropPartitionRequest dropPartitionRequest = DropPartitionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName())
                    .build();

            Status response = vectorSearchBrpc().drop_partition(dropPartitionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("DropPartitionRequest successfully! Collection name:{}, partition name:{}",
                        requestParam.getCollectionName(), requestParam.getPartitionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("DropPartitionRequest", response);
            }
        } catch (RpcExecutionException e) {
            logError("DropPartitionRequest RPC failed! Collection name:{}, partition name:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DropPartitionRequest failed! Collection name:{}, partition name:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<Boolean> hasPartition(@NonNull HasPartitionParam requestParam) {
        logInfo(requestParam.toString());

        try {
            HasPartitionRequest hasPartitionRequest = HasPartitionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName())
                    .build();

            BoolResponse response = vectorSearchBrpc().has_partition(hasPartitionRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("HasPartitionRequest successfully!");
                Boolean result = response.getValue();
                return R.success(result);
            } else {
                return failedStatus("HasPartitionRequest", response.getStatus());
            }
        } catch (RpcExecutionException e) {
            logError("HasPartitionRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("HasPartitionRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> loadPartitions(@NonNull LoadPartitionsParam requestParam) {
        logInfo(requestParam.toString());

        try {
            LoadPartitionsRequest.Builder builder = LoadPartitionsRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setReplicaNumber(requestParam.getReplicaNumber())
                    .addAllPartitionNames(requestParam.getPartitionNames())
                    .setRefresh(requestParam.isRefresh());
            LoadPartitionsRequest loadPartitionsRequest = builder.build();

            Status response = vectorSearchBrpc().load_partitions(loadPartitionsRequest);

            if (response.getErrorCode() != ErrorCode.Success) {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }

            // sync load, wait until all partitions finish loading
            if (requestParam.isSyncLoad()) {
                waitForLoadingCollection(requestParam.getDatabaseName(), requestParam.getCollectionName(), requestParam.getPartitionNames(),
                        requestParam.getSyncLoadWaitingInterval(), requestParam.getSyncLoadWaitingTimeout());
            }

            logDebug("LoadPartitionsRequest successfully! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (RpcExecutionException e) { // gRPC could throw this exception
            logError("LoadPartitionsRequest RPC failed! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e);
            return R.failed(e);
        } catch (Exception e) { // milvus exception for illegal response
            logError("LoadPartitionsRequest failed! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> releasePartitions(@NonNull ReleasePartitionsParam requestParam) {
        logInfo(requestParam.toString());

        try {
            ReleasePartitionsRequest releasePartitionsRequest = ReleasePartitionsRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .addAllPartitionNames(requestParam.getPartitionNames())
                    .build();

            Status response = vectorSearchBrpc().release_partitions(releasePartitionsRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("ReleasePartitionsRequest successfully! Collection name:{}, partition names:{}",
                        requestParam.getCollectionName(), requestParam.getPartitionNames());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("ReleasePartitionsRequest", response);
            }
        } catch (RpcExecutionException e) {
            logError("ReleasePartitionsRequest RPC failed! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("ReleasePartitionsRequest failed! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<ShowPartitionsResponse> showPartitions(@NonNull ShowPartitionsParam requestParam) {
        logInfo(requestParam.toString());

        try {
            ShowPartitionsRequest showPartitionsRequest = ShowPartitionsRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .addAllPartitionNames(requestParam.getPartitionNames())
                    .build();

            ShowPartitionsResponse response = vectorSearchBrpc().show_partitions(showPartitionsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("ShowPartitionsRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("ShowPartitionsRequest", response.getStatus());
            }
        } catch (RpcExecutionException e) {
            logError("ShowPartitionsRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("ShowPartitionsRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createIndex(@NonNull CreateIndexParam requestParam) {
        logInfo(requestParam.toString());

        try {
            // get collection schema to check input
            DescribeCollectionParam.Builder descBuilder = DescribeCollectionParam.newBuilder()
                    .withDatabaseName(requestParam.getDatabaseName())
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(descBuilder.build());

            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(R.Status.valueOf(descResp.getStatus()), descResp.getMessage());
            }

            DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());
            List<FieldType> fields = wrapper.getFields();
            // check field existence and index_type/field_type must be matched
            boolean fieldExists = false;
            boolean validType = false;
            for (FieldType field : fields) {
                if (requestParam.getFieldName().equals(field.getName())) {
                    fieldExists = true;
                    if (ParamUtils.VerifyIndexType(requestParam.getIndexType(), field.getDataType())) {
                        validType = true;
                    }
                    break;
                }
            }

            if (!fieldExists) {
                String msg = String.format("Field '%s' doesn't exist in the collection", requestParam.getFieldName());
                logError("CreateIndexRequest failed! {}\n", msg);
                return R.failed(R.Status.IllegalArgument, msg);
            }
            if (!validType) {
                String msg = String.format("Index type '%s' doesn't match with data type of field '%s'",
                        requestParam.getIndexType().name(), requestParam.getFieldName());
                logError("CreateIndexRequest failed! {}\n", msg);
                return R.failed(R.Status.IllegalArgument, msg);
            }

            // prepare index parameters
            CreateIndexRequest.Builder createIndexRequestBuilder = CreateIndexRequest.newBuilder();
            List<KeyValuePair> extraParamList = ParamUtils.AssembleKvPair(requestParam.getExtraParam());
            if (CollectionUtils.isNotEmpty(extraParamList)) {
                extraParamList.forEach(createIndexRequestBuilder::addExtraParams);
            }

            CreateIndexRequest.Builder builder = createIndexRequestBuilder
                    .setCollectionName(requestParam.getCollectionName())
                    .setFieldName(requestParam.getFieldName())
                    .setIndexName(requestParam.getIndexName());
            CreateIndexRequest createIndexRequest = builder.build();

            Status response = vectorSearchBrpc().create_index(createIndexRequest);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("CreateIndexRequest", response);
            }

            if (requestParam.isSyncMode()) {
                R<Boolean> res = waitForIndex(requestParam.getDatabaseName(), requestParam.getCollectionName(), requestParam.getIndexName(),
                        requestParam.getFieldName(),
                        requestParam.getSyncWaitingInterval(), requestParam.getSyncWaitingTimeout());
                if (res.getStatus() != R.Status.Success.getCode()) {
                    logError("CreateIndexRequest in sync mode" + " failed:{}", res.getMessage());
                    return R.failed(R.Status.valueOf(res.getStatus()), res.getMessage());
                }
            }
            logDebug("CreateIndexRequest successfully! Collection name:{}， Field name:{}",
                    requestParam.getCollectionName(), requestParam.getFieldName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (RpcExecutionException e) {
            logError("CreateIndexRequest RPC failed! Collection name:{}， Field name:{}",
                    requestParam.getCollectionName(), requestParam.getFieldName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateIndexRequest failed! Collection name:{} ，Field name:{}",
                    requestParam.getCollectionName(), requestParam.getFieldName(), e);
            return R.failed(e);
        }
    }

    private R<Boolean> waitForIndex(String databaseName, String collectionName, String indexName, String fieldName,
                                    long waitingInterval, long timeout) {
        // This method use getIndexState() to check index state.
        // If all index state become Finished, then we say the sync index action is finished.
        // If waiting time exceed timeout, exist the circle
        long tsBegin = System.currentTimeMillis();
        while (true) {
            long tsNow = System.currentTimeMillis();
            if ((tsNow - tsBegin) >= timeout * 1000) {
                String msg = "Waiting index thread is timeout, index process may not be finished";
                logWarning(msg);
                return R.failed(R.Status.UnexpectedError, msg);
            }

            DescribeIndexRequest.Builder builder = DescribeIndexRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setIndexName(indexName);
            DescribeIndexRequest request = builder.build();

            DescribeIndexResponse response = vectorSearchBrpc().describe_index(request);

            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return R.failed(response.getStatus().getErrorCode(), response.getStatus().getReason());
            }

            if (response.getIndexDescriptionsList().size() == 0) {
                return R.failed(R.Status.UnexpectedError, response.getStatus().getReason());
            }
            IndexDescription index = response.getIndexDescriptionsList().stream()
                    .filter(x -> x.getFieldName().equals(fieldName))
                    .findFirst()
                    .orElse(response.getIndexDescriptions(0));

            if (index.getState() == IndexState.Finished) {
                return R.success(true);
            } else if (index.getState() == IndexState.Failed) {
                String msg = "Get index state failed: " + index.getState().toString();
                logError(msg);
                return R.failed(R.Status.UnexpectedError, msg);
            }

            try {
                String msg = "Waiting index, interval: " + waitingInterval + "ms";
                logDebug(msg);
                TimeUnit.MILLISECONDS.sleep(waitingInterval);
            } catch (InterruptedException e) {
                String msg = "Waiting index thread is interrupted, index process may not be finished";
                logWarning(msg);
                return R.failed(R.Status.Success, msg);
            }
        }
    }

    @Override
    public R<DescribeIndexResponse> describeIndex(@NonNull DescribeIndexParam requestParam) {
        logInfo(requestParam.toString());

        try {
            DescribeIndexRequest.Builder builder = DescribeIndexRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName());
            DescribeIndexRequest describeIndexRequest = builder.build();

            DescribeIndexResponse response = vectorSearchBrpc().describe_index(describeIndexRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("DescribeIndexRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("DescribeIndexRequest", response.getStatus());
            }
        } catch (RpcExecutionException e) {
            logError("DescribeIndexRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DescribeIndexRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropIndex(@NonNull DropIndexParam requestParam) {
        logInfo(requestParam.toString());

        try {
            DropIndexRequest dropIndexRequest = DropIndexRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName())
                    .build();

            Status response = vectorSearchBrpc().drop_index(dropIndexRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("DropIndexRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("DropIndexRequest", response);
            }
        } catch (RpcExecutionException e) {
            logError("DropIndexRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DropIndexRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Deprecated
    // use DescribeIndex instead
    @Override
    public R<GetIndexStateResponse> getIndexState(@NonNull GetIndexStateParam requestParam) {
        logInfo(requestParam.toString());

        try {
            GetIndexStateRequest getIndexStateRequest = GetIndexStateRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName())
                    .build();

            GetIndexStateResponse response = vectorSearchBrpc().get_index_state(getIndexStateRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetIndexStateRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("GetIndexStateRequest", response.getStatus());
            }
        } catch (RpcExecutionException e) {
            logError("GetIndexStateRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetIndexStateRequest failed!", e);
            return R.failed(e);
        }
    }

    @Deprecated
    // use DescribeIndex instead
    @Override
    public R<GetIndexBuildProgressResponse> getIndexBuildProgress(@NonNull GetIndexBuildProgressParam requestParam) {
        logInfo(requestParam.toString());

        try {
            GetIndexBuildProgressRequest getIndexBuildProgressRequest = GetIndexBuildProgressRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName())
                    .build();

            GetIndexBuildProgressResponse response = vectorSearchBrpc().get_index_build_progress(getIndexBuildProgressRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetIndexBuildProgressRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("GetIndexBuildProgressRequest", response.getStatus());
            }
        } catch (RpcExecutionException e) {
            logError("GetIndexBuildProgressRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetIndexBuildProgressRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<MutationResult> insert(@NonNull InsertParam requestParam) {
        logInfo(requestParam.toString());

        try {
            DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                    .withDatabaseName(requestParam.getDatabaseName())
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(builder.build());

            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(R.Status.valueOf(descResp.getStatus()), descResp.getMessage());
            }

            DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());
            ParamUtils.InsertBuilderWrapper builderWraper = new ParamUtils.InsertBuilderWrapper(requestParam, wrapper);
            MutationResult response = vectorSearchBrpc().insert(builderWraper.buildInsertRequest());

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("InsertRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(response);
            } else {
                return failedStatus("InsertRequest", response.getStatus());
            }
        } catch (RpcExecutionException e) {
            logError("InsertRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("InsertRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<SearchResults> search(@NonNull SearchParam requestParam) {
        logInfo(requestParam.toString());

        try {
            SearchRequest searchRequest = ParamUtils.convertSearchParam(requestParam);
            SearchResults response = vectorSearchBrpc().search(searchRequest);

            //TODO: truncate distance value by round decimal

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("SearchRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("SearchRequest", response.getStatus());
            }
        } catch (RpcExecutionException e) {
            logError("SearchRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (ParamException e) {
            logError("SearchRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<QueryResults> query(@NonNull QueryParam requestParam) {
        logInfo(requestParam.toString());

        try {
            QueryRequest queryRequest = ParamUtils.convertQueryParam(requestParam);
            QueryResults response = this.vectorSearchBrpc().query(queryRequest);
            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("QueryRequest successfully!");
                return R.success(response);
            } else {
                // Server side behavior: if a query expression could not filter out any result,
                // or collection is empty, the server return ErrorCode.EmptyCollection.
                // Here we give a general message for this case.
                if (response.getStatus().getErrorCode() == ErrorCode.EmptyCollection) {
                    logWarning("QueryRequest returns nothing: empty collection or improper expression");
                    return R.failed(ErrorCode.EmptyCollection, "empty collection or improper expression");
                }
                return failedStatus("QueryRequest", response.getStatus());
            }
        } catch (RpcExecutionException e) {
//            e.printStackTrace();
            logError("QueryRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("QueryRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<MutationResult> delete(@NonNull DeleteParam requestParam) {
        logInfo(requestParam.toString());

        try {
            DeleteRequest deleteRequest = DeleteRequest.newBuilder()
                    .setBase(MsgBase.newBuilder().setMsgType(MsgType.Delete).build())
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName())
                    .setExpr(requestParam.getExpr())
                    .build();

            MutationResult response = vectorSearchBrpc().delete(deleteRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("DeleteRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(response);
            } else {
                return failedStatus("DeleteRequest", response.getStatus());
            }
        } catch (RpcExecutionException e) {
            logError("DeleteRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DeleteRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    private void waitForFlush(FlushResponse flushResponse, long waitingInterval, long timeout) {
        // The rpc api flush() return FlushResponse, but the returned segment ids maybe not yet persisted.
        // This method use getFlushState() to check segment state.
        // If all segments state become Flushed, then we say the sync flush action is finished.
        // If waiting time exceed timeout, exist the circle
        long tsBegin = System.currentTimeMillis();
        Map<String, LongArray> collectionSegIDs = flushResponse.getCollSegIDsMap();
        collectionSegIDs.forEach((collectionName, segmentIDs) -> {
            while (segmentIDs.getDataCount() > 0) {
                long tsNow = System.currentTimeMillis();
                if ((tsNow - tsBegin) >= timeout * 1000) {
                    logWarning("Waiting flush thread is timeout, flush process may not be finished");
                    break;
                }

                GetFlushStateRequest getFlushStateRequest = GetFlushStateRequest.newBuilder()
                        .addAllSegmentIDs(segmentIDs.getDataList())
                        .build();
                GetFlushStateResponse response = vectorSearchBrpc().get_flush_state(getFlushStateRequest);
                if (response.getFlushed()) {
                    // if all segment of this collection has been flushed, break this circle and check next collection
                    String msg = segmentIDs.getDataCount() + " segments of " + collectionName + " has been flushed";
                    logDebug(msg);
                    break;
                }

                try {
                    String msg = "Waiting flush for " + collectionName + ", interval: " + waitingInterval + "ms";
                    logDebug(msg);
                    TimeUnit.MILLISECONDS.sleep(waitingInterval);
                } catch (InterruptedException e) {
                    logWarning("Waiting flush thread is interrupted, flush process may not be finished");
                    break;
                }
            }
        });
    }

    @Override
    public R<FlushResponse> flush(@NonNull FlushParam requestParam) {
        logInfo(requestParam.toString());

        try {
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Flush).build();
            FlushRequest.Builder builder = FlushRequest.newBuilder()
                    .setBase(msgBase)
                    .addAllCollectionNames(requestParam.getCollectionNames());
            FlushRequest flushRequest = builder.build();
            FlushResponse response = vectorSearchBrpc().flush(flushRequest);

            if (Objects.equals(requestParam.getSyncFlush(), Boolean.TRUE)) {
                waitForFlush(response, requestParam.getSyncFlushWaitingInterval(),
                        requestParam.getSyncFlushWaitingTimeout());
            }

            logDebug("FlushRequest successfully! Collection names:{}", requestParam.getCollectionNames());
            return R.success(response);
        } catch (RpcExecutionException e) {
            logError("FlushRequest RPC failed! Collection names:{}",
                    requestParam.getCollectionNames(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("FlushRequest failed! Collection names:{}",
                    requestParam.getCollectionNames(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetFlushStateResponse> getFlushState(@NonNull GetFlushStateParam requestParam) {
        try {
            GetFlushStateRequest getFlushStateRequest = GetFlushStateRequest.newBuilder()
                    .addAllSegmentIDs(requestParam.getSegmentIDs())
                    .build();

            GetFlushStateResponse response = vectorSearchBrpc().get_flush_state(getFlushStateRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetFlushState successfully!");
                return R.success(response);
            } else {
                return failedStatus("GetFlushState", response.getStatus());
            }
        } catch (RpcExecutionException e) {
            logError("GetFlushState RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetFlushState failed!", e);
            return R.failed(e);
        }
    }

    protected void logDebug(String msg, Object... params) {
        if (logLevel.ordinal() <= LogLevel.Debug.ordinal()) {
            logger.debug(msg, params);
        }
    }

    protected void logInfo(String msg, Object... params) {
        if (logLevel.ordinal() <= LogLevel.Info.ordinal()) {
            logger.info(msg, params);
        }
    }

    protected void logWarning(String msg, Object... params) {
        if (logLevel.ordinal() <= LogLevel.Warning.ordinal()) {
            logger.warn(msg, params);
        }
    }

    protected void logError(String msg, Object... params) {
        if (logLevel.ordinal() <= LogLevel.Error.ordinal()) {
            logger.error(msg, params);
        }
    }
}
