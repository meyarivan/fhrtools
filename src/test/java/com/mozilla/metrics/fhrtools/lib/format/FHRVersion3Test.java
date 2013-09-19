package com.mozilla.metrics.fhrtools.lib.format;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.commons.io.FileUtils;

import java.io.File;

public class FHRVersion3Test {
  private FHRVersion3 fhrObj;

  @Before
  public void setUp() throws Exception {
    String jsonPath = "testv3.json";
    fhrObj = new FHRVersion3(FileUtils.readFileToString(new File(jsonPath)));
  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testGetVersion() throws Exception {
    assert(fhrObj.getVersion() == 3);
  }

  @Test
  public void testGetDailyData() throws Exception {

  }
}
