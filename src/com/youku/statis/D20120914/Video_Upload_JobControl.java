package com.youku.statis.D20120914;

import java.util.ArrayList;
import java.util.List;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.youku.statis.D20120914.Video_Upload_Statis.LogFilePathFilter;
import com.youku.statis.D20120914.Video_Upload_Statis.LogMapper;
import com.youku.statis.D20120914.Video_Upload_Statis.LogReducer;

public class Video_Upload_JobControl extends JobControl{
	
	
	
	public Video_Upload_JobControl(String groupName) {
		super(groupName);
	}
	
	@Override
	public void run(){
		
	}

	public static void main(String[] args) throws Exception{
		
		
		
		JobControl jobControl = new JobControl("Video Upload Jobs!");
		//jobControl.addJob(aJob);
		
		
		Configuration conf = new Configuration();
		conf.set("mapred.ignore.badcompress", "true");
		String[] inputArguments = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (inputArguments.length != 2) {
			System.err.println("Usage: videoupload <in> <out>");
			System.exit(2);
		}
		
		List<Job> jobList = new ArrayList<Job>();
		Collection<Job> jobCollection = new ArrayList<Job>();
		
		Job job = new Job(conf, "video upload statis.("+inputArguments[0]+"--"+inputArguments[1]+")");
		job.setJarByClass(Video_Upload_Statis.class);
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
		
		//jobCollection.add(job);
		
		//jobControl.addJobs(jobCollection);
		
		//Thread t = new Thread(jobControl);
		//t.start();
		
		
		//JobConf jobConf = new JobConf(conf, Video_Upload_Statis.class);
		
		
		
		JobClient jobClient = new JobClient();
		
		//jobClient.runJob(job);
		
	}
}
