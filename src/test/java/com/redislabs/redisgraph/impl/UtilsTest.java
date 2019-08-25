package com.redislabs.redisgraph.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.IllegalFormatConversionException;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

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
  
  @Test
  public void prepareQuery() {   
    Assert.assertEquals("query %s %d end of query", Utils.prepareQuery("query %s %d end of query"));
    
    Assert.assertEquals("query 'a' 33 end of query", Utils.prepareQuery("query %s %d end of query", "a", 33));
    
    try {
      Assert.assertEquals("CALL prc(\"a\",\"b\")ka,kb", Utils.prepareQuery("query %s %d end of query", "a", "b"));
      Assert.fail();
    } catch (IllegalFormatConversionException e) {}
  }

}
