package com.mozilla.metrics.fhrtools.lib.format;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

public class FHRVersion3 implements FHRData {
  private final SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-mm-dd");
  private final JSONObject root;
  private final short version;
  private byte[] hash;


  public FHRVersion3(final String jsonStr) throws UnableToParseException {
    try {
      root = JSON.parseObject(jsonStr);
      version = root.getShort("version");
    } catch (JSONException e) {
      throw new UnableToParseException();
    }
  }

  public final short getVersion() {
    return version;
  }

  public final JSONObject getDailyData() {
    return root.getJSONObject("data").getJSONObject("days");
  }

  public final Date getCurrentPingDate() throws ParseException {
    return dateFmt.parse(root.getString("thisPingDate"));
  }

  public final Date getLastPingDate() throws ParseException {
    return dateFmt.parse(root.getString("lastPingDate"));
  }

  public String toString() {
    return root.toString();
  }
}