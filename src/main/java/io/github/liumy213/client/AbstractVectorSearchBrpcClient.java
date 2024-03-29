package io.github.liumy213.client;

import com.baidu.fengchao.stargate.remoting.exceptions.RpcExecutionException;
import io.github.liumy213.exception.ParamException;
import io.github.liumy213.param.LogLevel;
import io.github.liumy213.param.ParamUtils;
import io.github.liumy213.param.R;
import io.github.liumy213.param.RpcStatus;
import io.github.liumy213.param.collection.*;
import io.github.liumy213.param.dml.InsertParam;
import io.github.liumy213.param.dml.SearchParam;
import io.github.liumy213.param.index.CreateIndexParam;
import io.github.liumy213.param.index.DropIndexParam;
import io.github.liumy213.response.DescCollResponseWrapper;
import io.github.liumy213.rpc.*;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public abstract class AbstractVectorSearchBrpcClient implements VectorSearchClient {
    protected static final Logger logger = LoggerFactory.getLogger(AbstractVectorSearchBrpcClient.class);
    protected LogLevel logLevel = LogLevel.Error;
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
            HasCollectionRequest hasCollectionRequest = builder
                    .build();

            HasCollectionResponse response = vectorSearchBrpc().has_collection(hasCollectionRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("HasCollectionRequest successfully!");
                Boolean value = Optional.of(response)
                        .map(HasCollectionResponse::getValue)
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
                    .setSchema(collectionSchemaBuilder.build().toBuilder());

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
    public R<RpcStatus> createIndex(@NonNull CreateIndexParam requestParam) {
        logInfo(requestParam.toString());

        try {
            // get collection schema to check input
            DescribeCollectionParam.Builder descBuilder = DescribeCollectionParam.newBuilder()
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

    @Override
    public R<RpcStatus> dropIndex(@NonNull DropIndexParam requestParam) {
        logInfo(requestParam.toString());

        try {
            DropIndexRequest dropIndexRequest = DropIndexRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setFieldName(requestParam.getFieldName())
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

    @Override
    public R<InsertResponse> insert(@NonNull InsertParam requestParam) {
        logInfo(requestParam.toString());

        try {
            DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(builder.build());

            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(R.Status.valueOf(descResp.getStatus()), descResp.getMessage());
            }

            DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());
            ParamUtils.InsertBuilderWrapper builderWraper = new ParamUtils.InsertBuilderWrapper(requestParam, wrapper);
            InsertResponse response = vectorSearchBrpc().insert_entity(builderWraper.buildInsertRequest());

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
    public R<SearchResponse> search(@NonNull SearchParam requestParam) {
        logInfo(requestParam.toString());

        try {
            SearchRequest searchRequest = ParamUtils.convertSearchParam(requestParam);
            SearchResponse response = vectorSearchBrpc().search_entity(searchRequest);

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
