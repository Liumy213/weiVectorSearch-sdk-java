package io.github.liumy213.response.basic;

import io.github.liumy213.exception.ParamException;
import io.github.liumy213.response.FieldDataWrapper;
import io.github.liumy213.response.RowRecord;
import io.github.liumy213.rpc.FieldData;

import java.util.List;

public abstract class RowRecordWrapper {

    public abstract List<RowRecord> getRowRecords();

    /**
     * Gets a row record from result.
     *  Throws {@link ParamException} if the index is illegal.
     *
     * @return <code>RowRecord</code> a row record of the result
     */
    protected RowRecord buildRowRecord(RowRecord record, long index) {
        for (String outputKey : getOutputFields()) {
            for (FieldData field : getFieldDataList()) {
                if (outputKey.equals(field.getFieldName())) {
                    FieldDataWrapper wrapper = new FieldDataWrapper(field);
                    if (index < 0 || index >= wrapper.getRowCount()) {
                        throw new ParamException("Index out of range");
                    }
                    Object value = wrapper.valueByIdx((int)index);
                    record.put(field.getFieldName(), value);
                    break;
                }
            }
        }
        return record;
    }

    protected abstract List<FieldData> getFieldDataList();
    protected abstract List<String> getOutputFields();

}
