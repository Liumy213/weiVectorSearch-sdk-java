package com.weibo.weivectorsearch.example;

import com.alibaba.fastjson.JSONObject;
import com.weibo.weivectorsearch.client.VectorSearchServiceClient;
import com.weibo.weivectorsearch.param.ConnectParam;
import com.weibo.weivectorsearch.param.IndexType;
import com.weibo.weivectorsearch.param.MetricType;
import com.weibo.weivectorsearch.param.R;
import com.weibo.weivectorsearch.param.collection.CreateCollectionParam;
import com.weibo.weivectorsearch.param.collection.FieldType;
import com.weibo.weivectorsearch.param.collection.LoadCollectionParam;
import com.weibo.weivectorsearch.param.dml.DeleteParam;
import com.weibo.weivectorsearch.param.dml.InsertParam;
import com.weibo.weivectorsearch.param.dml.SearchParam;
import com.weibo.weivectorsearch.param.index.CreateIndexParam;
import com.weibo.weivectorsearch.param.partition.CreatePartitionParam;
import com.weibo.weivectorsearch.rpc.DataType;
import com.weibo.weivectorsearch.rpc.ModelType;
import com.weibo.weivectorsearch.rpc.SearchResults;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.weibo.weivectorsearch.param.Constant.VECTOR_FIELD;

public class SimpleExample {
    public static void main(String[] args) {
        // create search service connect
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(18880)
                .build();
        VectorSearchServiceClient vectorSearchServiceClient = new VectorSearchServiceClient(connectParam);

        // Define fields
        String idFieldName = "id";
        FieldType idField = FieldType.newBuilder()
                .withName(idFieldName)
                .withDataType(DataType.Int64).build();

        String vectorFieldName = "vector";
        FieldType vectorField = FieldType.newBuilder()
                .withName(vectorFieldName)
                .withDataType(DataType.FloatVector)
                .withDimension(300)
                .build();

        String textFieldName = "text";
        FieldType textField = FieldType.newBuilder()
                .withName(textFieldName)
                .withDataType(DataType.String)
                .withModelType(ModelType.SIMCSE)
                .withMaxLength(512)
                .build();

        // Create the collection
        String collectionName = "collection_name";
        CreateCollectionParam createCollectionParam = CreateCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .addFieldType(idField)
                .addFieldType(vectorField)
                .addFieldType(textField)
                .build();
        vectorSearchServiceClient.createCollection(createCollectionParam);

        // Create the partition
        String partitionName = "partition_name";
        CreatePartitionParam createPartitionParam = CreatePartitionParam.newBuilder()
                .withCollectionName(collectionName)
                .withPartitionName(partitionName)
                .build();
        vectorSearchServiceClient.createPartition(createPartitionParam);

        // Create an index type on the vector field or text field.
        CreateIndexParam createIndexParam = CreateIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withFieldName(textFieldName)
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .build();
        vectorSearchServiceClient.createIndex(createIndexParam);

        // Call loadCollection() to enable automatically loading data into memory for searching
        LoadCollectionParam loadCollectionParam = LoadCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .build();
        vectorSearchServiceClient.loadCollection(loadCollectionParam);

        // Insert 1 text into the collection
        List<Long> idList = new ArrayList<>();
        idList.add(1234567890L);
        List<String> textRows = new ArrayList<>();
        textRows.add("向量检索也逐渐成了AI技术链路中不可或缺的一环");
        List<InsertParam.Field> textFields = new ArrayList<>();
        textFields.add(new InsertParam.Field(idFieldName, idList));
        textFields.add(new InsertParam.Field(textFieldName, textRows));
        InsertParam textInsertParam = InsertParam.newBuilder()
                .withCollectionName(collectionName)
                .withPartitionName(partitionName)
                .withFields(textFields)
                .build();
        vectorSearchServiceClient.insert(textInsertParam);

        // Insert 10 records into the collection
        List<Float> vectorList = new ArrayList<>();
        Random rand = new Random();
        for (int i = 0; i < 300; i++) {
            vectorList.add(rand.nextFloat());
        }
        List<InsertParam.Field> vectorFields = new ArrayList<>();
        vectorFields.add(new InsertParam.Field(idFieldName, idList));
        vectorFields.add(new InsertParam.Field(vectorFieldName, vectorList));
        InsertParam vectorInsertParam = InsertParam.newBuilder()
                .withCollectionName(collectionName)
                .withPartitionName(partitionName)
                .withFields(vectorFields)
                .build();
        vectorSearchServiceClient.insert(vectorInsertParam);

        // Search data from collection by text
        List<String> searchText = new ArrayList<>();
        searchText.add("向量检索便是对这类结构化的数据进行快速搜索和匹配的方法。");
        SearchParam textSearchParam = SearchParam.newBuilder()
                .withCollectionName(collectionName)
                .withMetricType(MetricType.L2)
                .withTopK(10)
                .withTexts(searchText)
                .withTextFieldName(textFieldName)
                .withParams("{}")
                .addOutField(idFieldName)
                .build();
        R<SearchResults> textSearchRet = vectorSearchServiceClient.search(textSearchParam);

        // Search data from collection by vector
        List<Float> searchVector = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            searchVector.add(rand.nextFloat());
        }
        SearchParam vectorSearchParam = SearchParam.newBuilder()
                .withCollectionName(collectionName)
                .withMetricType(MetricType.L2)
                .withTopK(10)
                .withVectors(Arrays.asList(searchVector))
                .withTextFieldName(textFieldName)
                .withParams("{}")
                .addOutField(idFieldName)
                .build();
        R<SearchResults> vectorSearchRet = vectorSearchServiceClient.search(vectorSearchParam);

        // Delete entity
        DeleteParam deleteParam = DeleteParam.newBuilder()
                .withCollectionName(collectionName)
                .withPartitionName(partitionName)
                .withExpr("id in [1234567890]")
                .build();
        vectorSearchServiceClient.delete(deleteParam);
    }
}
