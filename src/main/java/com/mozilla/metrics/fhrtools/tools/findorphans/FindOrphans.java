package com.mozilla.metrics.fhrtools.tools.findorphans;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mozilla.metrics.fhrtools.lib.format.FHRData;
import com.mozilla.metrics.fhrtools.lib.format.FHRVersion3;
import com.mozilla.metrics.fhrtools.lib.format.UnableToParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

class FindOrphans extends Configured implements Tool {
  public static String NAME = "FindOrphans";
  protected static String MOS_ORPHANS = "orphans";

  static Log LOG = LogFactory.getLog(FindOrphans.class);



  public FindOrphans(Configuration conf) throws Exception {
    super(conf);
  }

  private void usage() {
    System.err.println("usage: " + NAME + " inputdir outputdir");
  }

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();

    if (args.length < 2) {
      usage();
      System.exit(2);
    }

    String inputDir = args[0];
    String outputDir = args[1];

    Job job = new Job(conf);
    job.setJobName(FindOrphans.NAME + " : " + inputDir);

    job.setJarByClass(FindOrphanMapper.class);

    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    LOG.info("MAX VERSIONS = " + scan.getMaxVersions());

    job.setMapperClass(FindOrphanMapper.class);
    job.setReducerClass(FindOrphanReducer.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Result.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job, new Path(outputDir));
    FileInputFormat.addInputPath(job, new Path(inputDir));

    MultipleOutputs.addNamedOutput(job, MOS_ORPHANS,
      SequenceFileOutputFormat.class,
      Text.class, Text.class);
    boolean success = job.waitForCompletion(true);

    if (success) {

      return 0;
    }
    else
      return 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new FindOrphans(HBaseConfiguration.create()), args);

    System.exit(res);
  }
}
