package myflink;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class KeyValue {

    private String key;
    private Integer value;
    public KeyValue(String key, Integer value) {
        this.key = key;
        this.value = value;

    }

    public String getKey() {
        return this.key;
    }

    public Integer getValue() {
        return  this.value;
    }

    @Override
    public String toString() {
        return String.format("%s: %s", this.key, this.value);
    }
}

