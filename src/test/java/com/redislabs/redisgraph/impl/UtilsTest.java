package com.redislabs.redisgraph.impl;

import java.util.*;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class UtilsTest {
 
  @Test
  public void testPrepareProcedure() {   
    
    Assert.assertEquals("CALL prc()", Utils.prepareProcedure("prc", Arrays.asList(new String[]{}), new HashMap<>()));
    
    Assert.assertEquals("CALL prc(\"a\",\"b\")", Utils.prepareProcedure("prc", Arrays.asList(new String[]{"a", "b"}), new HashMap<>()));
    
    Map<String, List<String>> kwargs = new HashMap<>();
    kwargs.put("y", Arrays.asList(new String[]{"ka", "kb"}));
    Assert.assertEquals("CALL prc(\"a\",\"b\")ka,kb", Utils.prepareProcedure("prc", Arrays.asList(new String[]{"a", "b"}), kwargs));
    
    Assert.assertEquals("CALL prc()ka,kb", Utils.prepareProcedure("prc", Arrays.asList(new String[]{}), kwargs));
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();


  @Test
  public void prepareQuery() {   
    Assert.assertEquals("query %s %d end of query", Utils.prepareQuery("query %s %d end of query"));
    
    Assert.assertEquals("query 'a' 33 end of query", Utils.prepareQuery("query %s %d end of query", "a", 33));

    exceptionRule.expect(IllegalFormatConversionException.class);
    Assert.assertEquals("CAL prc(\"a\",\"b\")ka,kb", Utils.prepareQuery("query %s %d end of query", "a", "b"));
  }

  @Test
  public void testParamsPrep(){
    Map<String, Object> params = new HashMap<>();
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
    Integer arr[] = {1,2,3};
    params.put("param", arr);
    Assert.assertEquals("CYPHER param=[1, 2, 3] RETURN $param", Utils.prepareQuery("RETURN $param", params));
    List<Integer> list = Arrays.asList(1,2,3);
    params.put("param", list);
    Assert.assertEquals("CYPHER param=[1, 2, 3] RETURN $param", Utils.prepareQuery("RETURN $param", params));
  }

}
