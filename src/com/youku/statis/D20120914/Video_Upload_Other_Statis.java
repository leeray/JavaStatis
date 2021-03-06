package com.youku.statis.D20120914;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List; 
import java.io.InputStream; 
import java.net.URI;

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
import org.apache.hadoop.io.IOUtils; 

public class Video_Upload_Other_Statis {
	
	public static class LogMapper extends Mapper<Object, Text, Text, Video_Upload_Statis_Request> {
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Video_Upload_Statis_Request request = parseRequest(value.toString());
			if (request != null) {
				Text outKey_all = new Text();
				outKey_all.set(request.getDatestr() +" " + request.getPid() + " " + "ALL" + " " + "ALL" + " " + "ALL");
				context.write(outKey_all, request);
				
				Text outKey_ver = new Text();
				outKey_ver.set(request.getDatestr() +" " + request.getPid() + " " + "ALL" + " " + request.getProv() + " " + request.getCity());
				context.write(outKey_ver, request);
				
				Text outKey_prov = new Text();
				outKey_prov.set(request.getDatestr() +" " + request.getPid() + " " + request.getVer() + " " + "ALL" + " " + request.getCity());
				context.write(outKey_prov, request);
				
				Text outKey_city = new Text();
				outKey_city.set(request.getDatestr() +" " + request.getPid() + " " + request.getVer() + " " + request.getProv() + " " + "ALL");
				context.write(outKey_city, request);
				
				Text outKey_prov_city = new Text();
				outKey_prov_city.set(request.getDatestr() +" " + request.getPid() + " " + request.getVer() + " " + "ALL" + " " + "ALL");
				context.write(outKey_prov_city, request);
			}
		
		}

		private final Video_Upload_Statis_Request parseRequest(String line) throws IOException,
				InterruptedException {
			Video_Upload_Statis_Request request = new Video_Upload_Statis_Request(line);
			
			String pid = request.getPid();
			String datestr = request.getDatestr();
			if (request != null && (pid != null && !pid.equals("")) && (datestr != null && !datestr.equals(""))) {
				return request;
			} else {
				return null;
			}
		}

	}

	public static class LogPartitioner extends
			org.apache.hadoop.mapreduce.Partitioner<Text, Video_Upload_Statis_Request> {

		@Override
		public int getPartition(Text key, Video_Upload_Statis_Request request, int numPartitions) {
			return key.hashCode() % numPartitions;
		}
	}

	public static class LogReducer extends Reducer<Text, Video_Upload_Statis_Request, Text, Text> {

		public void reduce(Text key, Iterable<Video_Upload_Statis_Request> values, Context context)
				throws IOException, InterruptedException {
			java.util.Iterator<Video_Upload_Statis_Request> it = values.iterator();
			int pv = 0;
			List<PaikeVideo> pvList = new ArrayList<PaikeVideo>();
			Video_Upload_Statis_Request request = null;
			while (it.hasNext()) {
				request = it.next();
				pv += 1;
			}
			
			Text outValue = new Text();
			outValue.set(" " + pv);
			context.write(key, outValue);
		}
		
		public class PaikeVideo{
			private String pid;
			private String ver;
			private String prov;
			private String city;
			private String date;
			
			
		}
		
	}
	
	

	public static class LogFilePathFilter implements PathFilter {
		
		@Override
		public boolean accept(Path path){
			String pathName = path.getName();

			if (pathName.contains("part-")){;
				return true;
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
		if (inputArguments.length != 2) {
			System.err.println("Usage: videoupload_statisother <in> <out>");
			System.exit(2);
		}
		
		//conf.set("file", inputArguments[2]);
		
		Job job = new Job(conf, "video upload statis other.("+inputArguments[0]+"--"+inputArguments[1]+")");
		job.setJarByClass(Video_Upload_Other_Statis.class);
		job.setMapperClass(LogMapper.class);
		
		// job.setPartitionerClass(LogPartitioner.class);
		// job.setCombinerClass(LogReducer.class);
		job.setReducerClass(LogReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Video_Upload_Statis_Request.class);
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
	
}
