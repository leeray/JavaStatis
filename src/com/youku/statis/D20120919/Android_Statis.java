package com.youku.statis.D20120919;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Android_Statis {

	
	public static class LogMapper extends Mapper<Object, Text, Text, Android_Statis_Request> {
		private static final List<String> padPidList =new ArrayList<String>();
		private static final List<String> phonePidList =new ArrayList<String>();
		
		@Override
		protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException{
			Configuration conf = context.getConfiguration();
			this.initPadPid_hdfs(conf);
			this.initPhonePid_hdfs(conf);
		}
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			//InputSplit inputSplit = context.getInputSplit();
			String Android=""; //pad|phone
			String valueString = value.toString();
			if (valueString.contains("statis/vv")) {
				if (valueString.contains("root")) {
					String[] v = valueString.split("root");
					for (String line : v) {
						String newline = line.trim();
						if(newline == null || newline.equals("")){
							continue;
						}
						Android_Statis_Request request = parseRequest(newline);
						
						if (request != null && request.getResponse_code().equals("200")) {
							String pid = request.getPid();
							if(padPidList.contains(pid)){
								Android = "pad";
							}else if(phonePidList.contains(pid)){
								Android = "phone";
							}else{
								Android = "other";
							}
							
							Text outKey = new Text();
							outKey.set(Android+","+request.getVer()+","+request.getPlay_codes());
							context.write(outKey, request);
						}
					}
				} else {
					Android_Statis_Request request = parseRequest(value.toString());
					if (request != null && request.getResponse_code().equals("200")) {
						String pid = request.getPid();
						if(padPidList.contains(pid)){
							Android = "pad";
						}else if(phonePidList.contains(pid)){
							Android = "phone";
						}else{
							Android = "other";
						}
						
						Text outKey = new Text();
						outKey.set(Android+","+request.getVer()+","+request.getPlay_codes());
						context.write(outKey, request);
					}
				}
			}
		}

		private final Android_Statis_Request parseRequest(String line) throws IOException,
				InterruptedException {
			Android_Statis_Request request = new Android_Statis_Request(line);
			String pid = request.getPid();
			if (request != null && pid != null) {
				return request;
			} else {
				return null;
			}
		}
		
		private void initPadPid_hdfs(Configuration conf){
			String filename = conf.get("hdfs_pad");
			InputStream input  = null;  
	        try{  
	        	FileSystem fs = FileSystem.get(URI.create(filename), conf);  
	            input =  fs.open(new Path(filename)); 
	            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
	            String line = null;
	            if((line = reader.readLine()) != null){
	            	String[] pidArray = line.trim().split(",");
	            	for (String pid : pidArray){
	            		padPidList.add(pid.trim());
	            	}
	            }
	        }catch(IOException e){
	        	e.printStackTrace();
	        }finally{  
	            IOUtils.closeStream(input);  
	        }
		}
		
		private void initPhonePid_hdfs(Configuration conf){
			String filename = conf.get("hdfs_phone");
			InputStream input  = null;  
	        try{  
	        	FileSystem fs = FileSystem.get(URI.create(filename), conf);  
	            input =  fs.open(new Path(filename)); 
	            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
	            String line = null;
	            if((line = reader.readLine()) != null){
	            	String[] pidArray = line.trim().split(",");
	            	for (String pid : pidArray){
	            		phonePidList.add(pid.trim());
	            	}
	            }
	        }catch(IOException e){
	        	e.printStackTrace();
	        }finally{  
	            IOUtils.closeStream(input);  
	        }
		}

	}

	public static class LogPartitioner extends
			org.apache.hadoop.mapreduce.Partitioner<Text, Android_Statis_Request> {
		@Override
		public int getPartition(Text key, Android_Statis_Request request, int numPartitions) {
			return key.hashCode() % numPartitions;
		}
	}

	public static class LogReducer extends Reducer<Text, Android_Statis_Request, Text, Text> {

		public void reduce(Text key, Iterable<Android_Statis_Request> values, Context context)
				throws IOException, InterruptedException {
			java.util.Iterator<Android_Statis_Request> it = values.iterator();
			
			int pv = 0;
			Android_Statis_Request request = null;
			while (it.hasNext()) {
				request = it.next();
				
				pv += 1;
				
			}
			
			Text outValue = new Text();
			outValue.set(Integer.toString(pv));
			context.write(key, outValue);
		}
	}
	

	public static class LogFilePathFilter implements PathFilter {
		
		@Override
		public boolean accept(Path path){
			String pathName = path.getName();

			if (pathName.contains(".gz")){;
				return path.getName().endsWith("hash_api.tar.gz");
			}else{
				return true;
			}
		}
		
		static FileSystem fs;
		static Configuration conf;

		static void setConf(Configuration _conf) {
			conf = _conf;
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.ignore.badcompress", "true");
		String[] inputArguments = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (inputArguments.length != 4) {
			System.err.println("Usage: AndroidvvStatisComplete <in> <out> <hdfs://pad.pid> <hdfs://phone.pid>");
			System.exit(2);
		}
		
		conf.set("hdfs_pad", inputArguments[2]);
		conf.set("hdfs_phone", inputArguments[3]);
		
		Job job = new Job(conf, "android vv statis complete.("+inputArguments[0]+"--"+inputArguments[1]+")");
		job.setJarByClass(Android_Statis.class);
		job.setMapperClass(LogMapper.class);
		//job.setInputFormatClass(AndroidInputFormat.class);
		job.setReducerClass(LogReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Android_Statis_Request.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		LogFilePathFilter.setConf(conf);
		FileInputFormat.setInputPathFilter(job, LogFilePathFilter.class);

		FileSystem fstm = FileSystem.get(conf);
		Path outDir = new Path(args[1]);
		fstm.delete(outDir, true);

		job.setOutputFormatClass(AndroidOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputArguments[0]));
		FileOutputFormat.setOutputPath(job, new Path(inputArguments[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
