package com.youku.statis.D20120919;

import java.io.DataOutputStream;  
import java.io.IOException;  
import java.util.HashMap;  
import java.util.Iterator;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataOutputStream;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.Writable;  
import org.apache.hadoop.io.WritableComparable;  
import org.apache.hadoop.io.compress.CompressionCodec;  
import org.apache.hadoop.io.compress.GzipCodec;  
import org.apache.hadoop.mapreduce.OutputCommitter;  
import org.apache.hadoop.mapreduce.RecordWriter;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.ReflectionUtils; 


public abstract class MultipleOutputFormat<K extends WritableComparable<?>, V extends Writable>  
        extends FileOutputFormat<K, V> { //默认的是TextOutputFormat  
    private MultiRecordWriter writer = null;  
    
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException,  
            InterruptedException {  
        if (writer == null) {  
            writer = new MultiRecordWriter(job, getTaskOutputPath(job));//job ,output path  
        }  
        return writer;  
    }  
    
    private Path getTaskOutputPath(TaskAttemptContext conf) throws IOException {//获得输出路径  
        Path workPath = null;  
        OutputCommitter committer = super.getOutputCommitter(conf);  
        if (committer instanceof FileOutputCommitter) {//如果是  
            workPath = ((FileOutputCommitter) committer).getWorkPath();//工作路径  
        } else {  
            Path outputPath = super.getOutputPath(conf);//获得conf路径  
            if (outputPath == null) {  
                throw new IOException("Undefined job output-path");  
            }  
            workPath = outputPath;  
        }  
        return workPath; //  
    }  
    
    /**通过key, value, conf来确定输出文件名（含扩展名）*/  
    protected abstract String generateFileNameForKeyValue(K key, V value, Configuration conf);//抽象方法,被之后的方法重写了  
    
    public class MultiRecordWriter extends RecordWriter<K, V> {  
        /**RecordWriter的缓存*/  
        private HashMap<String, RecordWriter<K, V>> recordWriters = null;  
        private TaskAttemptContext job = null;  
        
        /**输出目录*/  
        private Path workPath = null;  
        public MultiRecordWriter(TaskAttemptContext job, Path workPath) {//构造函数  
            super();  
            this.job = job;
            this.workPath = workPath;  
            System.out.println("workPath.toString()000:"+workPath.toString());
            System.out.println("job.getConfiguration().get(\"hour\"):"+job.getConfiguration().get("hour"));
            System.out.println("job.getConfiguration().get(\"destpath\"):"+job.getConfiguration().get("destpath"));
            recordWriters = new HashMap<String, RecordWriter<K, V>>();  
        }  
        
        @Override  
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {//多个writer都要关掉  
            Iterator<RecordWriter<K, V>> values = this.recordWriters.values().iterator();  
            while (values.hasNext()) {  
                values.next().close(context);  
            }  
            this.recordWriters.clear();  
        }  
        
        @Override  
        public void write(K key, V value) throws IOException, InterruptedException {  
            //得到输出文件名  
            String baseName = generateFileNameForKeyValue(key, value, job.getConfiguration());//生成输出文件名  
            RecordWriter<K, V> rw = this.recordWriters.get(baseName);//？？  
            if (rw == null) {  
                rw = getBaseRecordWriter(job, baseName);//  
                this.recordWriters.put(baseName, rw);  
            }  
            rw.write(key, value);
        }  
        
        // ${mapred.out.dir}/_temporary/_${taskid}/${nameWithExtension}  
        private RecordWriter<K, V> getBaseRecordWriter(TaskAttemptContext job, String baseName)  
                throws IOException, InterruptedException {  
            Configuration conf = job.getConfiguration();  
            boolean isCompressed = getCompressOutput(job);  
            String keyValueSeparator = ",";  
            RecordWriter<K, V> recordWriter = null;  
            System.out.println("workPath.toString()111:"+workPath.toString());
            if (isCompressed) {  
                Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job,  
                        GzipCodec.class);  
                CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);  
                Path file = new Path(workPath, baseName + codec.getDefaultExtension());  
                FSDataOutputStream fileOut = file.getFileSystem(conf).create(file, false);  
                recordWriter = new LineRecordWriter<K, V>(new DataOutputStream(codec  
                        .createOutputStream(fileOut)), keyValueSeparator);  
            } else {  
                Path file = new Path(workPath, baseName);  
                FSDataOutputStream fileOut = file.getFileSystem(conf).create(file, false);//file 是指的file name of the output file  
                recordWriter = new LineRecordWriter<K, V>(fileOut, keyValueSeparator);//这里调用的LineRecordWriter  
            }  
            return recordWriter;  
        }  
    }  
}
