package com.redislabs.redisgraph.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.IllegalFormatConversionException;
import java.util.List;
import java.util.Map;

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

}
