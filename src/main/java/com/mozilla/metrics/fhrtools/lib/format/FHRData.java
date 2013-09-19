package com.mozilla.metrics.fhrtools.lib.format;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Writable;

public interface FHRData {
  public short getVersion();
  public JSONObject getDailyData();

}
