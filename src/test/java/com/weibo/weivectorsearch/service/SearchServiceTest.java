package com.weibo.weivectorsearch.service;

import com.weibo.weivectorsearch.client.VectorSearchServiceClient;
import com.weibo.weivectorsearch.param.ConnectParam;
import com.weibo.weivectorsearch.param.IndexType;
import com.weibo.weivectorsearch.param.MetricType;
import com.weibo.weivectorsearch.param.collection.*;
import com.weibo.weivectorsearch.param.R;
import com.weibo.weivectorsearch.param.dml.DeleteParam;
import com.weibo.weivectorsearch.param.dml.InsertParam;
import com.weibo.weivectorsearch.param.dml.SearchParam;
import com.weibo.weivectorsearch.param.index.CreateIndexParam;
import com.weibo.weivectorsearch.param.partition.ReleasePartitionsParam;
import com.weibo.weivectorsearch.rpc.DataType;
import com.weibo.weivectorsearch.rpc.ModelType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class SearchServiceTest {
    @Test
    public void test() {
        String host = "10.2.7.153";
        int port = 18880;



        String collectionName = "TestCollection";

        FieldType idField = FieldType.newBuilder()
                .withName("id")
                .withDataType(DataType.Int64).build();

        FieldType fieldTypeVector = FieldType.newBuilder()
                .withName("vector")
                .withDataType(DataType.FloatVector)
                .withDimension(300)
                .build();

        FieldType fieldTypeText = FieldType.newBuilder()
                .withName("text")
                .withDataType(DataType.String)
                .withModelType(ModelType.SIMCSE)
                .withMaxLength(512)
                .build();

//        List<InsertParam.Field> fields = new ArrayList<>();
//        fields.add(new InsertParam.Field(fieldTypeIDName, midList));
//        fields.add(new InsertParam.Field(fieldTypeCreatedTimeName, createdTimeList));
//        fields.add(new InsertParam.Field(fieldTypeVectorName, textVector));
//        fields.add(new InsertParam.Field(fieldTypeTextName, texts));
//        InsertParam insertParam = InsertParam.newBuilder()
//                .withCollectionName(collectionName)
//                .withPartitionName(insertPartitionName)
//                .withFields(fields)
//                .build();
//
        List<List<Float>> search_vectors = new ArrayList<>();
        String fieldTypeVectorName = "vectorField";
        List<String> textList = new ArrayList<>();
        String fieldTypeTextName = "textField";
        String SEARCH_PARAM = "{\"nprobe\":16}";
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(collectionName)
                .withMetricType(MetricType.IP)
                .withTopK(10)
                .withVectors(search_vectors)
                .withVectorFieldName(fieldTypeVectorName)
                .withTexts(textList)
                .withTextFieldName(fieldTypeTextName)
                .withParams(SEARCH_PARAM)
                .build();

        ConnectParam connectParam = ConnectParam.newBuilder().withHost(host).withPort(port).build();
        VectorSearchServiceClient vectorSearchServiceClient = new VectorSearchServiceClient(connectParam);

        R<Boolean> re = vectorSearchServiceClient.hasCollection(HasCollectionParam.newBuilder().withCollectionName(collectionName).build());
        System.out.println(re.getData().booleanValue());

        CreateCollectionParam createCollectionParam = CreateCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withDescription("test collection")
                .addFieldType(idField)
                .addFieldType(fieldTypeVector)
                .build();
        vectorSearchServiceClient.createCollection(createCollectionParam);

        vectorSearchServiceClient.dropCollection(
                DropCollectionParam.newBuilder()
                        .withCollectionName(collectionName).build()
        );

        vectorSearchServiceClient.loadCollection(
                LoadCollectionParam.newBuilder()
                        .withCollectionName(collectionName)
                        .build()
        );

        vectorSearchServiceClient.releasePartitions(
                ReleasePartitionsParam.newBuilder()
                        .withCollectionName(collectionName)
                        .build()
        );

        vectorSearchServiceClient.search(searchParam);

        DeleteParam deleteParam = DeleteParam.newBuilder()
                .withExpr("").build();
        vectorSearchServiceClient.delete(deleteParam);


    }
}