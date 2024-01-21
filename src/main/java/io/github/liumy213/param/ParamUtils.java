package io.github.liumy213.param;

import io.github.liumy213.common.utils.JacksonUtils;
import io.github.liumy213.exception.IllegalResponseException;
import io.github.liumy213.exception.ParamException;
import io.github.liumy213.param.collection.FieldType;
import io.github.liumy213.param.dml.InsertParam;
import io.github.liumy213.param.dml.SearchParam;
import io.github.liumy213.response.DescCollResponseWrapper;
import io.github.liumy213.rpc.*;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility functions for param classes
 */
public class ParamUtils {
    public static HashMap<DataType, String> getTypeErrorMsg() {
        final HashMap<DataType, String> typeErrMsg = new HashMap<>();
        typeErrMsg.put(DataType.None, "Type mismatch for field '%s': the field type is illegal");
        typeErrMsg.put(DataType.Bool, "Type mismatch for field '%s': Bool field value type must be Boolean");
        typeErrMsg.put(DataType.Int32, "Type mismatch for field '%s': Int32/Int16/Int8 field value type must be Short or Integer");
        typeErrMsg.put(DataType.Int64, "Type mismatch for field '%s': Int64 field value type must be Long");
        typeErrMsg.put(DataType.Float, "Type mismatch for field '%s': Float field value type must be Float");
        typeErrMsg.put(DataType.Double, "Type mismatch for field '%s': Double field value type must be Double");
        typeErrMsg.put(DataType.String, "Type mismatch for field '%s': String field value type must be String");
        typeErrMsg.put(DataType.FloatVector, "Type mismatch for field '%s': Float vector field's value type must be List<Float>");
        return typeErrMsg;
    }

    private static void checkFieldData(FieldType fieldSchema, InsertParam.Field fieldData) {
        List<?> values = fieldData.getValues();
        checkFieldData(fieldSchema, values);
    }

    private static void checkFieldData(FieldType fieldSchema, List<?> values) {
        HashMap<DataType, String> errMsgs = getTypeErrorMsg();
        DataType dataType = fieldSchema.getDataType();

        switch (dataType) {
            case FloatVector: {
                int dim = fieldSchema.getDimension();
                for (int i = 0; i < values.size(); ++i) {
                    // is List<> ?
                    Object value = values.get(i);
                    if (!(value instanceof List)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                    // is List<Float> ?
                    List<?> temp = (List<?>) value;
                    for (Object v : temp) {
                        if (!(v instanceof Float)) {
                            throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                        }
                    }

                    // check dimension
                    if (temp.size() != dim) {
                        String msg = "Incorrect dimension for field '%s': the no.%d vector's dimension: %d is not equal to field's dimension: %d";
                        throw new ParamException(String.format(msg, fieldSchema.getName(), i, temp.size(), dim));
                    }
                }
            }
            break;
            case Int64:
                for (Object value : values) {
                    if (!(value instanceof Long)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Int32:
                for (Object value : values) {
                    if (!(value instanceof Short) && !(value instanceof Integer)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Bool:
                for (Object value : values) {
                    if (!(value instanceof Boolean)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Float:
                for (Object value : values) {
                    if (!(value instanceof Float)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Double:
                for (Object value : values) {
                    if (!(value instanceof Double)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case String:
                for (Object value : values) {
                    if (!(value instanceof String)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            default:
                throw new IllegalResponseException("Unsupported data type returned by FieldData");
        }
    }

    /**
     * Checks if a string is empty or null.
     * Throws {@link ParamException} if the string is empty of null.
     *
     * @param target target string
     * @param name a name to describe this string
     */
    public static void CheckNullEmptyString(String target, String name) throws ParamException {
        if (target == null || StringUtils.isBlank(target)) {
            throw new ParamException(name + " cannot be null or empty");
        }
    }

    /**
     * Checks if a string is  null.
     * Throws {@link ParamException} if the string is null.
     *
     * @param target target string
     * @param name a name to describe this string
     */
    public static void CheckNullString(String target, String name) throws ParamException {
        if (target == null) {
            throw new ParamException(name + " cannot be null");
        }
    }

    /**
     * Checks if a metric is for float vector.
     *
     * @param metric metric type
     * @return boolean type
     */
    public static boolean IsFloatMetric(MetricType metric) {
        return metric == MetricType.L2 || metric == MetricType.IP || metric == MetricType.COSINE;
    }

    /**
     * Checks if a metric is for binary vector.
     *
     * @param metric metric type
     * @return boolean type
     */
    public static boolean IsBinaryMetric(MetricType metric) {
        return metric != MetricType.INVALID && !IsFloatMetric(metric);
    }

    /**
     * Checks if an index type is for vector field.
     *
     * @param idx index type
     * @return boolean type
     */
    public static boolean IsVectorIndex(IndexType idx) {
        return idx != IndexType.INVALID && idx.getCode() <= IndexType.DISKANN.getCode();
    }

    /**
     * Checks if an index type is matched with data type.
     *
     * @param indexType index type
     * @param dataType data type
     * @return boolean type
     */
    public static boolean VerifyIndexType(IndexType indexType, DataType dataType) {
        if (dataType == DataType.FloatVector || dataType == DataType.String) {
            return (IsVectorIndex(indexType));
        } else {
            return false;
        }
    }

    public static class InsertBuilderWrapper {
        private InsertRequest.Builder insertBuilder;

        public InsertBuilderWrapper(@NonNull InsertParam requestParam,
                                    DescCollResponseWrapper wrapper) {
            String collectionName = requestParam.getCollectionName();

            // generate insert request builder
            insertBuilder = InsertRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setNumRows(requestParam.getRowCount());
            fillFieldsData(requestParam, wrapper);
        }

        private void addFieldsData(FieldData value) {
            if (insertBuilder != null) {
                insertBuilder.addFieldsData(value);
            }
        }

        private void fillFieldsData(InsertParam requestParam, DescCollResponseWrapper wrapper) {
            // convert insert data
            List<InsertParam.Field> columnFields = requestParam.getFields();

            if (CollectionUtils.isNotEmpty(columnFields)) {
                checkAndSetColumnData(wrapper.getFields(), columnFields);
            }
        }

        private void checkAndSetColumnData(List<FieldType> fieldTypes, List<InsertParam.Field> fields) {
            // gen fieldData
            // make sure the field order must be consisted with collection schema
            for (FieldType fieldType : fieldTypes) {
                boolean found = false;
                for (InsertParam.Field field : fields) {
                    if (field.getName().equals(fieldType.getName())) {
                        checkFieldData(fieldType, field);

                        found = true;
                        this.addFieldsData(genFieldData(field.getName(), fieldType.getDataType(), field.getValues()));
                        break;
                    }

                }
                if (!found) {
                    String msg = "The field: " + fieldType.getName() + " is not provided.";
                    throw new ParamException(msg);
                }
            }
        }

        public InsertRequest buildInsertRequest() {
            if (insertBuilder != null) {
                return insertBuilder.build();
            }
            throw new ParamException("Unable to build insert request since no input");
        }
    }

    @SuppressWarnings("unchecked")
    public static SearchRequest convertSearchParam(@NonNull SearchParam requestParam) throws ParamException {
        SearchRequest.Builder builder = SearchRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName());

        // prepare target vectors
        List<?> searchData = requestParam.getSearchData();
        String vectorFieldName = requestParam.getVectorFieldName();
        String textFieldName = requestParam.getTextFieldName();
        if (vectorFieldName != null && !StringUtils.isBlank(vectorFieldName)) {
            if (searchData != null && searchData.size() > 0) {
                List<FloatArray> floatArrays = new ArrayList<>();
                for (Object vector : searchData) {
                    List<Float> list = (List<Float>) vector;
                    FloatArray floatArray = FloatArray.newBuilder().addAllData(list).build();
                    floatArrays.add(floatArray);
                }
                FloatArrayArray floatArrayArray = FloatArrayArray.newBuilder().addAllFloatVector(floatArrays).build();
                builder.setSearchVectors(floatArrayArray);
            }
        } else if (textFieldName != null && !StringUtils.isBlank(textFieldName)) {
            if (searchData != null && searchData.size() > 0) {
                List<String> list = (List<String>) searchData;
                StringArray stringArray = StringArray.newBuilder().addAllData(list).build();
                builder.setTexts(stringArray);
            }
        }

        builder.setNq(requestParam.getNQ());

        KeyValuePair keyValuePair = null;
        if (requestParam.getVectorFieldName() != null) {
            keyValuePair = KeyValuePair.newBuilder()
                    .setKey(Constant.VECTOR_FIELD)
                    .setValue(requestParam.getVectorFieldName())
                    .build();
        } else if (requestParam.getTextFieldName() != null) {
            keyValuePair = KeyValuePair.newBuilder()
                    .setKey(Constant.TEXT_FIELD)
                    .setValue(requestParam.getTextFieldName())
                    .build();
        }
        // search parameters
        builder.addSearchParams(keyValuePair)
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.TOP_K)
                                .setValue(String.valueOf(requestParam.getTopK()))
                                .build());

        if (null != requestParam.getParams() && !requestParam.getParams().isEmpty()) {
            try {
                Map<String, Object> paramMap = JacksonUtils.fromJson(requestParam.getParams(), Map.class);
                builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.PARAMS)
                                .setValue(requestParam.getParams())
                                .build());
            } catch (IllegalArgumentException e) {
                throw new ParamException(e.getMessage() + e.getCause().getMessage());
            }
        }

        if (!requestParam.getOutFields().isEmpty()) {
            requestParam.getOutFields().forEach(builder::addOutputFields);
        }

        builder.setDslType(DslType.BoolExprV1);
        if (requestParam.getExpr() != null && !requestParam.getExpr().isEmpty()) {
            builder.setDsl(requestParam.getExpr());
        }

        return builder.build();
    }

    private static final Set<DataType> vectorDataType = new HashSet<DataType>() {{
        add(DataType.FloatVector);
    }};

    private static FieldData genFieldData(String fieldName, DataType dataType, List<?> objects) {
        return genFieldData(fieldName, dataType, objects, Boolean.FALSE);
    }

    @SuppressWarnings("unchecked")
    private static FieldData genFieldData(String fieldName, DataType dataType, List<?> objects, boolean isDynamic) {
        if (objects == null) {
            throw new ParamException("Cannot generate FieldData from null object");
        }
        FieldData.Builder builder = FieldData.newBuilder();
        if (vectorDataType.contains(dataType)) {
            if (dataType == DataType.FloatVector) {
                List<Float> floats = new ArrayList<>();
                // each object is List<Float>
                for (Object object : objects) {
                    if (object instanceof List) {
                        List<Float> list = (List<Float>) object;
                        floats.addAll(list);
                    } else {
                        throw new ParamException("The type of FloatVector must be List<Float>");
                    }
                }

                int dim = floats.size() / objects.size();
                FloatArray floatArray = FloatArray.newBuilder().addAllData(floats).build();
                VectorField vectorField = VectorField.newBuilder().setDim(dim).setFloatVector(floatArray).build();
                return builder.setFieldName(fieldName).setType(DataType.FloatVector).setVectors(vectorField).build();
            }
        } else {
            switch (dataType) {
                case None:
                case UNRECOGNIZED:
                    throw new ParamException("Cannot support this dataType:" + dataType);
                case Int64: {
                    List<Long> longs = objects.stream().map(p -> (Long) p).collect(Collectors.toList());
                    LongArray longArray = LongArray.newBuilder().addAllData(longs).build();
                    ScalarField scalarField = ScalarField.newBuilder().setLongData(longArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
                }
                case Int32: {
                    List<Integer> integers = objects.stream().map(p -> p instanceof Short ? ((Short) p).intValue() : (Integer) p).collect(Collectors.toList());
                    IntArray intArray = IntArray.newBuilder().addAllData(integers).build();
                    ScalarField scalarField = ScalarField.newBuilder().setIntData(intArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
                }
                case Bool: {
                    List<Boolean> booleans = objects.stream().map(p -> (Boolean) p).collect(Collectors.toList());
                    BoolArray boolArray = BoolArray.newBuilder().addAllData(booleans).build();
                    ScalarField scalarField = ScalarField.newBuilder().setBoolData(boolArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
                }
                case Float: {
                    List<Float> floats = objects.stream().map(p -> (Float) p).collect(Collectors.toList());
                    FloatArray floatArray = FloatArray.newBuilder().addAllData(floats).build();
                    ScalarField scalarField = ScalarField.newBuilder().setFloatData(floatArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
                }
                case Double: {
                    List<Double> doubles = objects.stream().map(p -> (Double) p).collect(Collectors.toList());
                    DoubleArray doubleArray = DoubleArray.newBuilder().addAllData(doubles).build();
                    ScalarField scalarField = ScalarField.newBuilder().setDoubleData(doubleArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
                }
                case String: {
                    List<String> strings = objects.stream().map(p -> (String) p).collect(Collectors.toList());
                    StringArray stringArray = StringArray.newBuilder().addAllData(strings).build();
                    ScalarField scalarField = ScalarField.newBuilder().setStringData(stringArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
                }
            }
        }

        return null;
    }

    /**
     * Convert a grpc field schema to client field schema
     *
     * @param field FieldSchema object
     * @return {@link FieldType} schema of the field
     */
    public static FieldType ConvertField(@NonNull FieldSchema field) {
        FieldType.Builder builder = FieldType.newBuilder()
                .withName(field.getName())
                .withDescription(field.getDescription())
                .withDataType(field.getDataType());


        List<KeyValuePair> keyValuePairs = field.getTypeParamsList();
        keyValuePairs.forEach((kv) -> builder.addTypeParam(kv.getKey(), kv.getValue()));

        return builder.build();
    }

    /**
     * Convert a client field schema to grpc field schema
     *
     * @param field {@link FieldType} object
     * @return {@link FieldSchema} schema of the field
     */
    public static FieldSchema ConvertField(@NonNull FieldType field) {
        FieldSchema.Builder builder = FieldSchema.newBuilder()
                .setName(field.getName())
                .setDescription(field.getDescription())
                .setDataType(field.getDataType())
                .setModelType(field.getModelType());

        // assemble typeParams for CollectionSchema
        List<KeyValuePair> typeParamsList = AssembleKvPair(field.getTypeParams());
        if (CollectionUtils.isNotEmpty(typeParamsList)) {
            typeParamsList.forEach(builder::addTypeParams);
        }

        return builder.build();
    }

    public static List<KeyValuePair> AssembleKvPair(Map<String, String> sourceMap) {
        List<KeyValuePair> result = new ArrayList<>();

        if (MapUtils.isNotEmpty(sourceMap)) {
            sourceMap.forEach((key, value) -> {
                KeyValuePair kv = KeyValuePair.newBuilder()
                        .setKey(key)
                        .setValue(value).build();
                result.add(kv);
            });
        }
        return result;
    }

    @Builder
    @Getter
    public static class InsertDataInfo {
        private final String fieldName;
        private final DataType dataType;
        private final LinkedList<Object> data;
    }
}
