package io.github.liumy213.response;

import io.github.liumy213.exception.ParamException;
import io.github.liumy213.param.ParamUtils;
import io.github.liumy213.param.collection.FieldType;
import io.github.liumy213.rpc.CollectionSchema;
import io.github.liumy213.rpc.DataType;
import io.github.liumy213.rpc.DescribeCollectionResponse;
import io.github.liumy213.rpc.FieldSchema;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Util class to wrap response of <code>describeCollection</code> interface.
 */
public class DescCollResponseWrapper {
    private final DescribeCollectionResponse response;

    public DescCollResponseWrapper(@NonNull DescribeCollectionResponse response) {
        this.response = response;
    }

    /**
     * Get name of the collection.
     *
     * @return <code>String</code> name of the collection
     */
    public String getCollectionName() {
        CollectionSchema schema = response.getSchema();
        return schema.getName();
    }

    /**
     * Get description of the collection.
     *
     * @return <code>String</code> description of the collection
     */
    public String getCollectionDescription() {
        CollectionSchema schema = response.getSchema();
        return schema.getDescription();
    }

    /**
     * Get internal id of the collection.
     *
     * @return <code>long</code> internal id of the collection
     */
    public long getCollectionID() {
        return response.getCollectionID();
    }

    /**
     * Get shard number of the collection.
     *
     * @return <code>int</code> shard number of the collection
     */
    public int getShardNumber() {
        return response.getShardsNum();
    }

    /**
     * Get utc timestamp when collection created.
     *
     * @return <code>long</code> utc timestamp when collection created
     */
    public long getCreatedUtcTimestamp() {
        return response.getCreatedUtcTimestamp();
    }

    /**
     * Get schema of the collection's fields.
     *
     * @return List of FieldType, schema of the collection's fields
     */
    public List<FieldType> getFields() {
        List<FieldType> results = new ArrayList<>();
        CollectionSchema schema = response.getSchema();
        List<FieldSchema> fields = schema.getFieldsList();
        fields.forEach((field) -> results.add(ParamUtils.ConvertField(field)));

        return results;
    }

    /**
     * Get schema of a field by name.
     * Return null if the field doesn't exist
     *
     * @param fieldName field name to get field description
     * @return {@link FieldType} schema of the field
     */
    public FieldType getFieldByName(@NonNull String fieldName) {
        CollectionSchema schema = response.getSchema();
        for (int i = 0; i < schema.getFieldsCount(); ++i) {
            FieldSchema field = schema.getFields(i);
            if (fieldName.compareTo(field.getName()) == 0) {
                return ParamUtils.ConvertField(field);
            }
        }

        return null;
    }

    /**
     * Get the partition key field.
     * Return null if the partition key field doesn't exist.
     *
     * @return {@link FieldType} schema of the partition key field
     */
    public FieldType getPartitionKeyField() {
        CollectionSchema schema = response.getSchema();
        for (int i = 0; i < schema.getFieldsCount(); ++i) {
            FieldSchema field = schema.getFields(i);
            if (field.getIsPartitionKey()) {
                return ParamUtils.ConvertField(field);
            }
        }

        return null;
    }

    /**
     * Get the primary key field.
     * throw ParamException if the primary key field doesn't exist.
     *
     * @return {@link FieldType} schema of the primary key field
     */
    public FieldType getPrimaryField() {
        CollectionSchema schema = response.getSchema();
        for (int i = 0; i < schema.getFieldsCount(); ++i) {
            FieldSchema field = schema.getFields(i);
            if (field.getIsPrimaryKey()) {
                return ParamUtils.ConvertField(field);
            }
        }

        throw new ParamException("No primary key found.");
    }

    /**
     * Get the vector key field.
     * throw ParamException if the vector key field doesn't exist.
     *
     * @return {@link FieldType} schema of the vector key field
     */
    public FieldType getVectorField() {
        CollectionSchema schema = response.getSchema();
        for (int i = 0; i < schema.getFieldsCount(); ++i) {
            FieldSchema field = schema.getFields(i);
            if (field.getDataType() == DataType.FloatVector) {
                return ParamUtils.ConvertField(field);
            }
        }

        throw new ParamException("No vector key found.");
    }

    /**
     * Construct a <code>String</code> by {@link DescCollResponseWrapper} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "Collection Description{" +
                "name:'" + getCollectionName() + '\'' +
                ", description:'" + getCollectionDescription() + '\'' +
                ", id:" + getCollectionID() +
                ", shardNumber:" + getShardNumber() +
                ", createdUtcTimestamp:" + getCreatedUtcTimestamp() +
                ", fields:" + getFields().toString() +
                '}';
    }
}
