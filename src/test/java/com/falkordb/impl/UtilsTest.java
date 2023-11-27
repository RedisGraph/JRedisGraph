package com.falkordb.impl;

import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.HashMap;
import java.util.IllegalFormatConversionException;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.falkordb.impl.Utils;

public class UtilsTest {

    @Test
    public void testPrepareProcedure() {
        Assert.assertEquals("CALL prc()",
                Utils.prepareProcedure("prc", Arrays.asList(new String[]{}), new HashMap<>()));

        Assert.assertEquals("CALL prc(\"a\",\"b\")",
                Utils.prepareProcedure("prc", Arrays.asList(new String[]{"a", "b"}), new HashMap<>()));

        Map<String, List<String>> kwargs = new HashMap<>();
        kwargs.put("y", Arrays.asList(new String[]{"ka", "kb"}));
        Assert.assertEquals("CALL prc(\"a\",\"b\")ka,kb",
                Utils.prepareProcedure("prc", Arrays.asList(new String[]{"a", "b"}), kwargs));

        Assert.assertEquals("CALL prc()ka,kb", Utils.prepareProcedure("prc", Arrays.asList(new String[]{}), kwargs));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void prepareQuery() {
        Assert.assertEquals("query %s %d end of query", Utils.prepareQuery("query %s %d end of query"));

        Assert.assertEquals("query 'a' 33 end of query", Utils.prepareQuery("query %s %d end of query", "a", 33));

        assertThrows(IllegalFormatConversionException.class,
                () -> Utils.prepareQuery("query %s %d end of query", "a", "b"));
    }

    @Test
    public void testParamsPrep() {
        Map<String, Object> params = new HashMap<>();
        params.put("param", "");
        Assert.assertEquals("CYPHER param=\"\" RETURN $param", Utils.prepareQuery("RETURN $param", params));
        params.put("param", "\"");
        Assert.assertEquals("CYPHER param=\"\\\"\" RETURN $param", Utils.prepareQuery("RETURN $param", params));
        params.put("param", "\"st");
        Assert.assertEquals("CYPHER param=\"\\\"st\" RETURN $param", Utils.prepareQuery("RETURN $param", params));
        params.put("param", 1);
        Assert.assertEquals("CYPHER param=1 RETURN $param", Utils.prepareQuery("RETURN $param", params));
        params.put("param", 2.3);
        Assert.assertEquals("CYPHER param=2.3 RETURN $param", Utils.prepareQuery("RETURN $param", params));
        params.put("param", true);
        Assert.assertEquals("CYPHER param=true RETURN $param", Utils.prepareQuery("RETURN $param", params));
        params.put("param", false);
        Assert.assertEquals("CYPHER param=false RETURN $param", Utils.prepareQuery("RETURN $param", params));
        params.put("param", null);
        Assert.assertEquals("CYPHER param=null RETURN $param", Utils.prepareQuery("RETURN $param", params));
        params.put("param", "str");
        Assert.assertEquals("CYPHER param=\"str\" RETURN $param", Utils.prepareQuery("RETURN $param", params));
        params.put("param", "s\"tr");
        Assert.assertEquals("CYPHER param=\"s\\\"tr\" RETURN $param", Utils.prepareQuery("RETURN $param", params));
        Integer arr[] = {1, 2, 3};
        params.put("param", arr);
        Assert.assertEquals("CYPHER param=[1, 2, 3] RETURN $param", Utils.prepareQuery("RETURN $param", params));
        List<Integer> list = Arrays.asList(1, 2, 3);
        params.put("param", list);
        Assert.assertEquals("CYPHER param=[1, 2, 3] RETURN $param", Utils.prepareQuery("RETURN $param", params));
        String strArr[] = {"1", "2", "3"};
        params.put("param", strArr);
        Assert.assertEquals("CYPHER param=[\"1\", \"2\", \"3\"] RETURN $param",
                Utils.prepareQuery("RETURN $param", params));
        List<String> stringList = Arrays.asList("1", "2", "3");
        params.put("param", stringList);
        Assert.assertEquals("CYPHER param=[\"1\", \"2\", \"3\"] RETURN $param",
                Utils.prepareQuery("RETURN $param", params));
    }

}
