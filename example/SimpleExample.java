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
        VectorSearchServiceClient client = new VectorSearchServiceClient(connectParam);

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
        // vector field and text field can only be selected to create one,
        // because if the text field is created, the system will generate the
        // corresponding vector for creation, and cannot create two vector fields at the same time in the engine
        String collectionName = "collection_example";
        CreateCollectionParam createCollectionParam = CreateCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .addFieldType(idField)
                .addFieldType(textField)
                .build();
        client.createCollection(createCollectionParam);

        // Create the partition
        String partitionName = "partition_example";
        CreatePartitionParam createPartitionParam = CreatePartitionParam.newBuilder()
                .withCollectionName(collectionName)
                .withPartitionName(partitionName)
                .build();
        client.createPartition(createPartitionParam);

        // Create an index type on the vector field or text field.
        String INDEX_PARAM = "{\"nlist\":1024}";
        CreateIndexParam createIndexParam = CreateIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withFieldName(textFieldName)
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.IP)
                .withExtraParam(INDEX_PARAM)
                .build();
        client.createIndex(createIndexParam);

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
        client.insert(textInsertParam);

        // Search data from collection by text
        List<String> searchText = new ArrayList<>();
        searchText.add("向量检索便是对这类结构化的数据进行快速搜索和匹配的方法。");
        SearchParam textSearchParam = SearchParam.newBuilder()
                .withCollectionName(collectionName)
                .withMetricType(MetricType.L2)
                .withTopK(10)
                .withTexts(searchText)
                .withTextFieldName(textFieldName)
                .addOutField(idFieldName)
                .build();
        R<SearchResults> textSearchRet = client.search(textSearchParam);
        SearchResultsWrapper wrapperSearch = new SearchResultsWrapper(textSearchRet.getData().getResults());
        for (int k = 0; k < searchText.size(); k++) {
            List<?> searchIdList = wrapperSearch.getFieldData(idFieldName, k);
            List<SearchResultsWrapper.IDScore> scoreList = wrapperSearch.getIDScore(k);
            int resultSize = searchIdList.size();
            for (int i = 0; i < resultSize; i++) {
                System.out.println("id: " + searchIdList.get(i));
                System.out.println("score: " + scoreList.get(i).getScore());
            }
        }

        // drop index
        DropIndexParam dropIndexParam = DropIndexParam.newBuilder()
                .withCollectionName(collectionName).build();
        client.dropIndex(dropIndexParam);

        // drop partition
        DropPartitionParam dropPartitionParam = DropPartitionParam.newBuilder()
                .withCollectionName(collectionName)
                .withPartitionName(partitionName).build();
        client.dropPartition(dropPartitionParam);

        // droop collection
        DropCollectionParam dropCollectionParam = DropCollectionParam.newBuilder()
                .withCollectionName(collectionName1).build();
        client.dropCollection(dropCollectionParam);
    }
}
