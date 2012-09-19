package com.youku.statis.D20120919;

import java.io.DataOutputStream;  
import java.io.IOException;  
import java.io.UnsupportedEncodingException;  
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.RecordWriter;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  

/**摘自{@link TextOutputFormat}中的LineRecordWriter。 */  
public class LineRecordWriter<K, V> extends RecordWriter<K, V> {  
    private static final String utf8 = "UTF-8";  
    private static final byte[] newline;  
    
    static {  
        try {  
            newline = "\n".getBytes(utf8);// 相当与分隔符  
        } catch (UnsupportedEncodingException uee) {  
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");  
        }  
    }  
    
    protected DataOutputStream out;  
    private final byte[] keyValueSeparator;  
    
    public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {  
        this.out = out;  
        try {  
            this.keyValueSeparator = keyValueSeparator.getBytes(utf8);  
        } catch (UnsupportedEncodingException uee) {  
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");  
        }  
    }  
    
    public LineRecordWriter(DataOutputStream out) {  
        this(out, ",");//"/t"默认的分隔符  
    }  
    
    private void writeObject(Object o) throws IOException {//被write函数调用  
        if (o instanceof Text) {//  
            Text to = (Text) o;  
            out.write(to.getBytes(), 0, to.getLength());//将指定 byte 数组中从偏移量 off 开始的 len 个字节写入基础输出流  
        } else {  
            out.write(o.toString().getBytes(utf8));  
        }  
    }  
    
    public synchronized void write(K key, V value) throws IOException {//这个要修改成 只是写成一个文件的格式,  
        boolean nullKey = key == null || key instanceof NullWritable;  
        boolean nullValue = value == null || value instanceof NullWritable;//重点是要改写Key，value，之类，value是一个文本，key是地址，这里不写入key了  
        if (nullKey && nullValue) {  
            return;  
        }  
        if (!nullKey) {//这个可以控制是否写入key,seperate and value 
        	String keys = key.toString();
        	keys = keys.substring(keys.indexOf(",")+1);
            writeObject(keys); 
        } 
        if (!(nullKey || nullValue)) { 
            out.write(keyValueSeparator); 
        }  
        if (!nullValue) {  
            writeObject(value);  
        }  
        out.write(newline);  
    }  
    
    public synchronized void close(TaskAttemptContext context) throws IOException {  
        out.close();  
    }  
}
