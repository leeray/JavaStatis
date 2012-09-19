package com.youku.statis.D20120912;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List; 
import java.io.InputStream; 
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IOUtils; 

public class Ipad_NU_Track_Statis {
	
	public static class LogMapper extends Mapper<Object, Text, Text, Ipad_NU_Track_Statis_Request> {
		private static final List<String> guidList =new ArrayList<String>();
		
		@Override
		protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException{
			Configuration conf = context.getConfiguration();
			//String filename = conf.get("file");
			//this.initGuid_Mapper(filename);
			this.initGuid_hdfs(conf);
		}
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String valueString = value.toString();
			if (valueString.contains("root")) {
				String[] v = valueString.split("root");
				for (String line : v) {
					String newline = line.trim();
					if(newline == null || newline.equals("")){
						continue;
					}
					Ipad_NU_Track_Statis_Request request = parseRequest(newline);
					if (request != null && request.getResponse_code().equals("200")) {
						Text outKey = new Text();
						outKey.set(request.getPid()+" "+request.getGuid()+" "+request.getUri());
						context.write(outKey, request);
					}
				}
			} else {
				Ipad_NU_Track_Statis_Request request = parseRequest(value.toString());
				if (request != null && request.getResponse_code().equals("200")) {
					Text outKey = new Text();
					outKey.set(request.getPid()+" "+request.getGuid()+" "+request.getUri());
					context.write(outKey, request);
				}
			}
		}

		private final Ipad_NU_Track_Statis_Request parseRequest(String line) throws IOException,
				InterruptedException {
			Ipad_NU_Track_Statis_Request request = new Ipad_NU_Track_Statis_Request(line);
			String pid = request.getPid();
			if (pid==null || (!pid.equals("87c959fb273378eb") && !pid.equals("a4f46b4582fa09f3") && !pid.equals("a8f2373285115c07"))) {
				return null;
			}
			
			String guid = request.getGuid();
			if(guid==null || !guidList.contains(guid)){
				return null;
			}
			return request;
		}
		
		private void initGuid_Mapper(String guid_nu_file) {
	        BufferedReader reader = null;
	        try {
	            File f = new File(guid_nu_file);
	            reader = new BufferedReader(new FileReader(f));
	            String line = null;
	            while((line = reader.readLine()) != null){
	                guidList.add(line.trim());
	            }
	        } catch (IOException e) {
	            e.printStackTrace();
	        }finally{
	            if(reader != null){
	                try {
	                    reader.close();
	                } catch (IOException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	    }
		
		private void initGuid_hdfs(Configuration conf){
			//Configuration conf = context.getConfiguration();
			String filename = conf.get("file");
			InputStream input  = null;  
	        try{  
	        	FileSystem fs = FileSystem.get(URI.create(filename), conf);  
	            input =  fs.open(new Path(filename)); 
	            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
	            String line = null;
	            while((line = reader.readLine()) != null){
	                guidList.add(line.trim());
	            }
	            //IOUtils.copyBytes(input, System.out, 4096, false);  
	        }catch(IOException e){
	        	e.printStackTrace();
	        }finally{  
	            IOUtils.closeStream(input);  
	        }
		}

	}

	public static class LogPartitioner extends
			org.apache.hadoop.mapreduce.Partitioner<Text, Ipad_NU_Track_Statis_Request> {

		@Override
		public int getPartition(Text key, Ipad_NU_Track_Statis_Request request, int numPartitions) {
			return key.hashCode() % numPartitions;
		}
	}

	public static class LogReducer extends Reducer<Text, Ipad_NU_Track_Statis_Request, Text, Text> {

		public void reduce(Text key, Iterable<Ipad_NU_Track_Statis_Request> values, Context context)
				throws IOException, InterruptedException {
			java.util.Iterator<Ipad_NU_Track_Statis_Request> it = values.iterator();
			int pv = 0;
			String ua = "";
			Ipad_NU_Track_Statis_Request request = null;
			while (it.hasNext()) {
				request = it.next();
				ua = request.getUser_agent();
				pv += 1;
			}
			
			Text outValue = new Text();
			outValue.set(" \"" + ua + "\" " + pv);
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
		if (inputArguments.length != 3) {
			System.err.println("Usage: ipadtrack <in> <out> <guid_nu_file>");
			System.exit(2);
		}
		
		conf.set("file", inputArguments[2]);
		
		Job job = new Job(conf, "ipad nu track.("+inputArguments[0]+"--"+inputArguments[1]+")");
		job.setJarByClass(Ipad_NU_Track_Statis.class);
		job.setMapperClass(LogMapper.class);
		
		// job.setPartitionerClass(LogPartitioner.class);
		// job.setCombinerClass(LogReducer.class);
		job.setReducerClass(LogReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Ipad_NU_Track_Statis_Request.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		LogFilePathFilter.setConf(conf);
		FileInputFormat.setInputPathFilter(job, LogFilePathFilter.class);

		FileSystem fstm = FileSystem.get(conf);
		Path outDir = new Path(args[1]);
		fstm.delete(outDir, true);

		FileInputFormat.addInputPath(job, new Path(inputArguments[0]));
		FileOutputFormat.setOutputPath(job, new Path(inputArguments[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void setGuidFile(String guid_nu_file) {
		//initGuid(guid_nu_file);
	}
}
