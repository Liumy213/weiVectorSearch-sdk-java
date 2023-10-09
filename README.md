# weiVectorSearch java sdk

Java sdk for weiVectorSearch

# Getting Started

## Install Java SDK
You can use Apache Maven to download the SDK.

```shell
<dependency>
    <groupId>io.github</groupId>
    <artifactId>weiVectorSearch-sdk-java</artifactId>
    <version>0.0.1</version>
</dependency>
```

## Usage


### Connect to server

Follow the example below to connect to the service using host and port

```java
ConnectParam connectParam = ConnectParam.newBuilder()
        .withHost("localhost")
        .withPort(18880)
        .build();
VectorSearchServiceClient vectorSearchServiceClient = new VectorSearchServiceClient(connectParam);
```
### Define field

Define the required fields such as id, vector, and text.

The service can store and retrieve the vector directly from the defined string field.You don't need to generate the vector separately, you can also use the vector directly
```java
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
```

### Create a collection
Create a collection after connecting to the service
```java
String collectionName = "collection_name";
CreateCollectionParam createCollectionParam = CreateCollectionParam.newBuilder()
        .withCollectionName(collectionName)
        .addFieldType(idField)
        .addFieldType(vectorField)
        .addFieldType(textField)
        .build();
vectorSearchServiceClient.createCollection(createCollectionParam);
```

### Create index
The service creates indexes with different parameters
```java
CreateIndexParam createIndexParam = CreateIndexParam.newBuilder()
        .withCollectionName(collectionName)
        .withFieldName(textFieldName)
        .withIndexType(IndexType.IVF_FLAT)
        .withMetricType(MetricType.L2)
        .build();
vectorSearchServiceClient.createIndex(createIndexParam);
```

### Insert data
Inserting single or multiple pieces of data
```java
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
```

### Search data
Search a single or multiple pieces of data
```java
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
```

### Delete data
Delete data from the service
```java
DeleteParam deleteParam = DeleteParam.newBuilder()
        .withCollectionName(collectionName)
        .withPartitionName(partitionName)
        .withExpr("id in [1234567890]")
        .build();
vectorSearchServiceClient.delete(deleteParam);
```