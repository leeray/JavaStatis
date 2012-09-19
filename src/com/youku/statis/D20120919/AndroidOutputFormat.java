package com.youku.statis.D20120919;

import org.apache.hadoop.conf.Configuration;   
import org.apache.hadoop.io.Text;  
  
  
public class AndroidOutputFormat extends MultipleOutputFormat<Text, Text> {  
      
    private final static String suffix = ".log";  
      
    //用key作为文件名
    @Override  
    protected String generateFileNameForKeyValue(Text key, Text value, Configuration conf) {  
        // TODO Auto-generated method stub  
    	//String dirpath = conf.get("hour");
//    	System.out.println("dirPath:"+dirpath);
//    	System.out.println("destPath:"+conf.get("destpath"));
        String path =  key.toString(); //文件的路径及名字 
        String filename = path.split(",")[0];
        //String[] dir = path.split("/");
          
        //int length = dir.length;   
        //String filename = dir[length -1];  
        //filename = filename.substring(0, filename.indexOf("_"));
        //System.out.println("ZipOutputFormat filename:"+filename);
        return filename + suffix;//输出的文件名
    }
  
      
}  
