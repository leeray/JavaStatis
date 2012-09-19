package com.youku.statis.D20120919;

import java.io.IOException;  

import org.apache.commons.logging.Log;  
import org.apache.commons.logging.LogFactory;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataInputStream;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.io.compress.CompressionCodec;  
import org.apache.hadoop.io.compress.CompressionCodecFactory;  
  
import org.apache.hadoop.mapreduce.InputSplit;  
import org.apache.hadoop.mapreduce.RecordReader;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;  
import org.apache.hadoop.mapreduce.lib.input.FileSplit;  
  
  
public class FileRecordReader extends RecordReader<Text, Text> {  
      
    private static final Log LOG = LogFactory.getLog(FileRecordReader.class.getName());  
    private CompressionCodecFactory compressionCodecs = null;  
    private long start;  
    private long pos;  
    private long end;  
    private byte[] buffer;  
    private String keyName;  
    private FSDataInputStream fileIn;  
    private Text key = null;  
    private Text value = null;  
  
    public FileRecordReader(TaskAttemptContext context, InputSplit genericSplit) throws IOException {  
        // TODO Auto-generated constructor stub  
          
        Configuration job = context.getConfiguration();  
        FileSplit split = (FileSplit) genericSplit;  
        start = ((FileSplit) split).getStart(); //从中可以看出每个文件是作为一个split的  
        end = split.getLength() + start;  
        final Path path = split.getPath();//  
        keyName = path.toString();//key 的值是文件路径  
        LOG.info("filename in hdfs is : " + keyName);//写入日志文件,去哪里查看日志呢？  
        final FileSystem fs = path.getFileSystem(job);  
        fileIn = fs.open(path);  
        fileIn.seek(start);  
        buffer = new byte[(int)(end - start)];  
        this.pos = start;  
        /*if(key == null){ 
            key = new Text(); 
            key.set(keyName); 
        } 
        if(value == null){ 
            value = new Text(); 
            value.set(utf8); 
        }*/  
          
          
    }//coAuRecordReader()  
  
      
  
    @Override  
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)  
            throws IOException, InterruptedException {  
        // TODO Auto-generated method stub  
        FileSplit split = (FileSplit) genericSplit;  
        Configuration job = context.getConfiguration();  
        //this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);  
        start = split.getStart();  
        end = start + split.getLength();  
        final Path file = split.getPath();  
        compressionCodecs = new CompressionCodecFactory(job);  
        final CompressionCodec codec = compressionCodecs.getCodec(file);  
        keyName = file.toString();//key 的值是文件路径  
        LOG.info("filename in hdfs is : " + keyName);//写入日志文件,去哪里查看日志呢？  
        final FileSystem fs = file.getFileSystem(job);  
        fileIn = fs.open(file);  
        fileIn.seek(start);  
        buffer = new byte[(int)(end - start)];  
        this.pos = start;  
          
          
    }  
  
    @Override  
    public boolean nextKeyValue() throws IOException, InterruptedException {  
        // TODO Auto-generated method stub  
        //这个是需要做的  
          
        if(key == null){  
            key = new Text();  
        }  
        key.set(keyName);  
        if(value == null){  
            value = new Text();  
        }  
        key.clear();  
        key.set(keyName);// set the key  
        value.clear();//clear the value  
        while(pos < end){  
            fileIn.readFully(pos,buffer);  
            value.set(buffer);  
              
            pos += buffer.length;  
//            System.out.println("end is : " + end  + " pos is : " + pos);  
            return true;  
        }  
          
          
        return false;  
    }  
  
    @Override  
    public Text getCurrentKey() throws IOException, InterruptedException {  
        // TODO Auto-generated method stub  
        return key;  
    }  
  
    @Override  
    public Text getCurrentValue() throws IOException, InterruptedException {  
        // TODO Auto-generated method stub  
        return value;  
    }  
  
    @Override  
    public float getProgress() throws IOException, InterruptedException {  
        // TODO Auto-generated method stub  
        if (start == end) {  
            return 0.0f;  
        } else {  
            return Math.min(1.0f, (pos - start) / (float)(end - start));  
        }  
    }  
  
    @Override  
    public void close() throws IOException {  
        // TODO Auto-generated method stub  
        if (fileIn != null) {  
            fileIn.close();   
        }  
    }  
  
      
  
}