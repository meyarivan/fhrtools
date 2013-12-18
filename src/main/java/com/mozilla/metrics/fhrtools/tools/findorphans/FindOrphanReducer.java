package com.mozilla.metrics.fhrtools.tools.findorphans;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

class FindOrphanReducer extends Reducer<ImmutableBytesWritable, Result, Text, Text> {

  static Log LOG = LogFactory.getLog(FindOrphanReducer.class);
  private final byte[] colFamily = Bytes.toBytes("data");
  private final byte[] colId = Bytes.toBytes("json");
  private final DateTimeFormatter dateFmt = DateTimeFormat.forPattern("yyyy-MM-dd");

  private MessageDigest md;
  private MultipleOutputs mos;
  public enum Counters { ORPHANS, EXACT_COPIES, INVALID_DAYS_DATA };
  private enum Decision { CHOOSE_FIRST, CHOOSE_SECOND, DUPLICATES, UNDECIDED, UNRELATED };
  private long orphans = 0, exactCopies=0, invalidDaysData=0;
  private Text keyObj = new Text(), valObj = new Text();


  private byte[] getData(final Result p) {
    return p.getValue(colFamily, colId);
  }

  private JSONObject getJSON(final Result p) {
    return JSON.parseObject(Bytes.toString(getData(p)));

  }

  @Override
  protected void reduce(final ImmutableBytesWritable key, final Iterable<Result> values,
                        final Context context) throws IOException, java.lang.InterruptedException {

    for (Map.Entry<Result, Boolean> entry: filterOrphans(values).entrySet()) {
      Result b = entry.getKey();

      keyObj.set(b.getRow());
      valObj.set(getData(b));

      if (entry.getValue() == false) {
        context.write(keyObj, valObj);
      }
      else {
        mos.write(FindOrphans.MOS_ORPHANS, keyObj, valObj, "orphans/part");
        orphans += 1;
      }
    }
  }

  private Map<Result, Boolean> filterOrphans(final Iterable<Result> values) throws IOException {
    HashMap<Result, Boolean> all = new HashMap<Result, Boolean>();
    HashMap<Result, TreeMap<LocalDate, byte[]>> checksums = new HashMap<Result, TreeMap<LocalDate, byte[]>>();

    for (Result r : values) {
      Result np = new Result();
      np.copyFrom(r);
      all.put(np, false);
      checksums.put(np, getChecksums(np));
    }

    if (all.size() == 1)
      return all;

    Result pi, pj, orphan;
    final Result[] allKeys = all.keySet().toArray(new Result[0]);

    for (int i = 0; i < all.size(); i++) {
      pi = allKeys[i];

      for (int j = i+1; j < all.size(); j++) {
        pj = allKeys[j];
        orphan = null;

        Decision cmp = compareChecksums(checksums.get(pi), checksums.get(pj));

        if (cmp == Decision.UNRELATED) {
          continue;
        } else if (cmp == Decision.CHOOSE_FIRST) {
          orphan = pj;
        } else if (cmp == Decision.CHOOSE_SECOND) {
          orphan = pi;
        }
        else if (cmp == Decision.DUPLICATES) {
          Decision d = resolveDups(getJSON(pi), getJSON(pj));

          if (d == Decision.CHOOSE_SECOND) {
            orphan = pi;
          }
          else if (d == Decision.CHOOSE_FIRST) {
            orphan = pj;
          }
          else if (d == Decision.DUPLICATES) {
            exactCopies += 1;
            if (pi.getColumnLatest(colFamily, colId).getTimestamp() < pj.getColumnLatest(colFamily, colId).getTimestamp()) { // TODO Fix Criterion
              orphan = pj;
            }
            else {
              orphan = pi;
            }
          }
        }

        if (orphan != null) {
          all.put(orphan, true);
        }
      }

    }
    return all;
  }


  LocalDate parseDate(String dateStr) {
    return dateFmt.parseDateTime(dateStr).toLocalDate();
  }

  private Decision resolveDups(final JSONObject objA, final JSONObject objB) {   // TODO refactor+cleanup

    if (Arrays.equals(md.digest(Bytes.toBytes(objA.toString())),  md.digest(Bytes.toBytes(objB.toString())))) {
      return Decision.DUPLICATES;
    } else {
      LocalDate aCurrentPingDate, aLastPingDate, bCurrentPingDate, bLastPingDate;

      try {
        aCurrentPingDate = parseDate(objA.getString("thisPingDate"));
        aLastPingDate = parseDate(objA.getString("lastPingDate"));

        bCurrentPingDate = parseDate(objB.getString("thisPingDate"));
        bLastPingDate = parseDate(objB.getString("lastPingDate"));
      } catch (IllegalArgumentException e) {
        return Decision.UNDECIDED;
      }
      if (aCurrentPingDate.equals(bLastPingDate)) {
        return Decision.CHOOSE_SECOND;
      } else if (bCurrentPingDate.equals(aLastPingDate)) {
        return Decision.CHOOSE_FIRST;
      }
      else if (aLastPingDate.equals(bLastPingDate)) {
        if (aCurrentPingDate.isBefore(bCurrentPingDate)) {
          return Decision.CHOOSE_SECOND;
        }
        else if (aCurrentPingDate.isAfter(bCurrentPingDate)) {
          return Decision.CHOOSE_FIRST;
        }
      }
      else if (aLastPingDate.isBefore(bLastPingDate) && aCurrentPingDate.isBefore(bCurrentPingDate)){
        return Decision.CHOOSE_SECOND;
      }
      else if (aLastPingDate.isAfter(bLastPingDate) && aCurrentPingDate.isAfter(bCurrentPingDate)) {
        return Decision.CHOOSE_FIRST;
      }
      else {
        System.err.println(objA.getJSONObject("environments").size() + " " + objB.getJSONObject("environments").size() + " " + aCurrentPingDate.isAfter(bCurrentPingDate) + " " + aLastPingDate.isAfter(bLastPingDate));
        System.err.println(objA.toString());
        System.err.println(objB.toString());
        return Decision.UNDECIDED;
      }

    }
    return Decision.UNDECIDED;
  }

  @Override
  public void cleanup(final Context context) throws IOException, InterruptedException {
    context.getCounter(Counters.ORPHANS).increment(orphans);
    context.getCounter(Counters.EXACT_COPIES).increment(exactCopies);
    context.getCounter(Counters.INVALID_DAYS_DATA).increment(invalidDaysData);
    mos.close();
  }

  private Decision compareChecksums(final TreeMap<LocalDate, byte[]> a, final TreeMap<LocalDate, byte[]> b) {
    TreeMap<LocalDate, byte[]> shorter, longer;
    boolean swapped = false;

    if (a.size() > b.size()) {
      shorter = b;
      longer = a;
      swapped = true;
    } else {
      shorter = a;
      longer = b;
    }

    for (LocalDate d : shorter.keySet()) {
      if (longer.containsKey(d)) {
        if (! Arrays.equals(shorter.get(d), longer.get(d))) {
          return Decision.UNRELATED;
        }
      } else {
        return Decision.UNRELATED;
      }
    }

    if (swapped) {
      return Decision.CHOOSE_FIRST;
    } else if (a.size() <  b.size()) {
      return Decision.CHOOSE_SECOND;
    } else {
      return Decision.DUPLICATES;
    }
  }

  protected void setup(final Context context) {
    try {
      md = MessageDigest.getInstance("SHA");
    } catch (NoSuchAlgorithmException e) {
      LOG.error(e.getMessage());
    }

    mos = new MultipleOutputs(context);
  }

  private TreeMap<LocalDate, byte[]> getChecksums(final Result p) {

    TreeMap<LocalDate, byte[]> ret = new TreeMap<LocalDate, byte[]>();

    JSONObject json = getJSON(p);
    JSONObject data = json.getJSONObject("data").getJSONObject("days"); // TODO

    for (String s : data.keySet()) {
      try {
        LocalDate d = parseDate(s);
        ret.put(d, md.digest(data.getJSONObject(s).toString().getBytes()));
      } catch (IllegalArgumentException e) {
        LOG.warn("Invalid date for row " + Bytes.toString(p.getRow()) + " " + e.getMessage());
      } catch (ClassCastException e) {
        System.err.println("Unable to compute hash for day " + s + " " + json.toString());
        invalidDaysData += 1;
      }
    }

    return ret;
  }
}

