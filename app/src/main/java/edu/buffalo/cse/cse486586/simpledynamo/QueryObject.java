package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

public class QueryObject implements Serializable {
    String key;
    String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public QueryObject(String key, String value){
        this.key = key;
        this.value = value;
    }
}