package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


@SuppressWarnings("rawtypes")
public class BatchEnrichmentMapper<K extends WritableComparable, V extends Writable> 
extends MapReduceBase implements Mapper<K, V, K, V> {

  
  private String mapTaskId;
  private String inputFile;
  public void configure(JobConf job) {
    mapTaskId = job.get("mapred.task.id");
    inputFile = job.get("map.input.file");
  }
  
  public void map(K key, V val,
                  OutputCollector<K, V> output, Reporter reporter)
  throws IOException {
    // Process the <key, value> pair (assume this takes a while)
    // ...
    // ...
    
    // Let the framework know that we are alive, and kicking!
    // reporter.progress();
    
    // Process some more
    // ...
    // ...
    
   
    // Output the result
    output.collect(key, val);
  }
}