package com.diorsding.spark.utils;

import org.junit.Test;

/**
 * Created by jiashan on 12/29/16.
 */
public class CassandraUtilsTest {

    @Test
    public void setupDatabase() {
        CassandraUtils.setupCanssadraTables();
    }
}
