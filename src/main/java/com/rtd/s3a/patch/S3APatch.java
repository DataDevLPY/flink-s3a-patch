package com.rtd.s3a.patch;

import org.apache.flink.table.functions.ScalarFunction;

public class S3APatch extends ScalarFunction {
    public S3APatch() {
    }

    public String eval(String input) {
        return input;
    }
}
