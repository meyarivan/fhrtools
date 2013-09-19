package com.mozilla.metrics.fhrtools.tools.findorphans;


import com.alibaba.fastjson.JSONObject;
import com.mozilla.metrics.fhrtools.lib.format.FHRData;
import com.mozilla.metrics.fhrtools.lib.format.FHRVersion3;
import com.mozilla.metrics.fhrtools.lib.format.UnableToParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;

class FindOrphanMapper extends Mapper<Text, Text, BytesWritable, Put> {

  static final Log LOG = LogFactory.getLog(FindOrphans.class);
  static final String HASH_ALGORITHM = "SHA";

  public enum Counters {FAILED_TO_PARSE, NO_DAYS_DATA};
  private long nRecords = 0, nFailed = 0, noData = 0;
  private final byte[] colFamily = Bytes.toBytes("data");
  private final byte[] colId = Bytes.toBytes("json");
  private final DateTimeFormatter dateFmt = DateTimeFormat.forPattern("yyyy-MM-dd");
  private MessageDigest md;


  @Override
  protected void map(Text row, Text value, Context context) throws InterruptedException, IOException {

    try {
      FHRData m = new FHRVersion3(value.toString());

      if (m.getVersion() != 3)
        return;

      JSONObject data = m.getDailyData();

      LocalDate oldest = null;
      ArrayList<LocalDate> days = new ArrayList<LocalDate>();

      for (String s : data.keySet()) {
        try {
          days.add(dateFmt.parseDateTime(s).toLocalDate());
        } catch (IllegalArgumentException e) {
          LOG.warn("Error Parsing Date in row " + row + " " + e.getMessage());
        }
      }
      if (days.size() == 0) {
        noData += 1;
        return;
      }
      oldest = Collections.min(days);
      context.write(new BytesWritable(md.digest((dateFmt.print(oldest) + data.get(dateFmt.print(oldest)).toString()).getBytes())), createPut(row, value));
    } catch (UnableToParseException e) {
      nFailed += 1;
      return;
    }

  }

  private Put createPut(Text row, Text value) {
    Put p = new Put(row.copyBytes());
    p.add(colFamily, colId, value.copyBytes());
    return p;

  }
  @Override
  protected void setup(Context context) {
    System.err.println("Invoking Setup()");
    try {
      md = MessageDigest.getInstance(HASH_ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      LOG.error(e.getMessage());
    }
  }

  @Override
  protected void cleanup(final Context context) throws IOException {
    context.getCounter(Counters.FAILED_TO_PARSE).increment(nFailed);
    context.getCounter(Counters.NO_DAYS_DATA).increment(noData);
  }
}

