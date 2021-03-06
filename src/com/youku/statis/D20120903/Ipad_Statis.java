package com.youku.statis.D20120903;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Ipad_Statis {
	
	public static class LogMapper extends Mapper<Object, Text, Text, Ipad_Statis_Request> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			//InputSplit inputSplit = context.getInputSplit();
			
			String valueString = value.toString();
			if (valueString.contains("root")) {
				String[] v = valueString.split("root");
				for (String line : v) {
					String newline = line.trim();
					if(newline == null || newline.equals("")){
						continue;
					}
					Ipad_Statis_Request request = parseRequest(newline);
					if (request != null && request.getResponse_code().equals("200")) {
						Text outKey = new Text();
						outKey.set(request.getGuid());
						context.write(outKey, request);
					}
				}
			} else {
				Ipad_Statis_Request request = parseRequest(value.toString());
				if (request != null && request.getResponse_code().equals("200")) {
					Text outKey = new Text();
					outKey.set(request.getGuid());
					context.write(outKey, request);
				}
			}
		}

		private final Ipad_Statis_Request parseRequest(String line) throws IOException,
				InterruptedException {
			Ipad_Statis_Request request = new Ipad_Statis_Request(line);
			String pid = request.getPid();
			if (pid==null || (!pid.equals("87c959fb273378eb") && !pid.equals("a4f46b4582fa09f3") && !pid.equals("a8f2373285115c07"))) {
				return null;
			}
			return request;
		}

	}

	public static class LogPartitioner extends
			org.apache.hadoop.mapreduce.Partitioner<Text, Ipad_Statis_Request> {

		@Override
		public int getPartition(Text key, Ipad_Statis_Request request, int numPartitions) {
			return key.hashCode() % numPartitions;
		}
	}

	public static class LogReducer extends Reducer<Text, Ipad_Statis_Request, Text, Text> {

		public void reduce(Text key, Iterable<Ipad_Statis_Request> values, Context context)
				throws IOException, InterruptedException {
			java.util.Iterator<Ipad_Statis_Request> it = values.iterator();
			int vv = 0;
			int pv = 0;
			
			Ipad_Statis_Request request = null;
			while (it.hasNext()) {
				request = it.next();
				pv += 1;
				if (request.isVv_statis()) {
					vv += 1;
				}
			}
			
			Text outValue = new Text();
			outValue.set(" " + pv + " " + vv);
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
		if (inputArguments.length != 2) {
			System.err.println("Usage: ipadstatis <in> <out>");
			System.exit(2);
		}
		
		
		Job job = new Job(conf, "ipad statis.("+inputArguments[0]+"--"+inputArguments[1]+")");
		job.setJarByClass(Ipad_Statis.class);
		job.setMapperClass(LogMapper.class);
		// job.setPartitionerClass(LogPartitioner.class);
		// job.setCombinerClass(LogReducer.class);
		job.setReducerClass(LogReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Ipad_Statis_Request.class);
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
