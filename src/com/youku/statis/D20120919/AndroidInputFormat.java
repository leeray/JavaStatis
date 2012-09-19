package com.youku.statis.D20120919;

import java.io.IOException;  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.io.compress.CompressionCodec;  
import org.apache.hadoop.io.compress.CompressionCodecFactory;  
import org.apache.hadoop.mapreduce.InputSplit;  
import org.apache.hadoop.mapreduce.RecordReader;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
  
  
  
  
  
public class AndroidInputFormat extends FileInputFormat<Text, Text>{  
    private CompressionCodecFactory compressionCodecs = null;  
    public void configure(Configuration conf) {  
        compressionCodecs = new CompressionCodecFactory(conf);  
    }  
      
    /** 
     * @brief isSplitable 不对文件进行切分，必须对文件整体进行处理 
     * 
     * @param fs 
     * @param file 
     * 
     * @return false 
     */  
    protected boolean isSplitable(FileSystem fs, Path file) {  
        CompressionCodec codec = compressionCodecs.getCodec(file);  
        return false;//以文件为单位，每个单位作为一个split，即使单个文件的大小超过了64M，也就是Hadoop一个块得大小，也不进行分片  
    }  
  
    @Override  
    public RecordReader<Text, Text> createRecordReader(InputSplit split,  
            TaskAttemptContext context) throws IOException,  
            InterruptedException {  
        // TODO Auto-generated method stub  
        return new FileRecordReader(context, split);  
    }  
  
}