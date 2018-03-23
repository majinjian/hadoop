package com.supinfo.transcode.launcher;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.supinfo.transcode.auxobject.PacketComparator;
import com.supinfo.transcode.auxobject.PacketInfo;
import com.supinfo.transcode.ioformat.MediaInputFormat;
import com.supinfo.transcode.ioformat.MediaOutputFormat;
import com.supinfo.transcode.mapreduce.XMapper;
import com.supinfo.transcode.mapreduce.XReducer;

public class Launcher {
	
	private static String inputPath;

	private static String outputPath;
	
	private static String inputFile;
	
	private static String outputFile;
	
	private static final String JOB_NAME = "transcode";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		inputPath = args[0];
		inputFile = args[1];
		outputPath = args[2];
		outputFile = args[3];
		
		//the configuration of mapreduce
		Configuration conf = new Configuration();
		conf.set("inputPath", inputPath);
		conf.set("inputFile", inputFile);
		conf.set("outputFile", outputFile);
		conf.set("outputPath", outputPath);
		conf.set("format", outputFile.split("\\.")[1]);
	    Job job = Job.getInstance(conf, JOB_NAME);
	    job.setJarByClass(Launcher.class);
		//settings for mapper
		job.setMapperClass(XMapper.class);
		job.setInputFormatClass(MediaInputFormat.class);
		job.setMapOutputKeyClass(PacketInfo.class);
		job.setMapOutputValueClass(BytesWritable.class);
		
		//set the key output key comparator
		job.setSortComparatorClass(PacketComparator.class);
		
		//settings for reducer
		job.setReducerClass(XReducer.class);
		job.setOutputKeyClass(PacketInfo.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setOutputFormatClass(MediaOutputFormat.class);
		job.setNumReduceTasks(1);
		
		//source data and output data path
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job,new Path(outputPath));
		
		//fire the map reduce job
		System.out.println(job.waitForCompletion(true));
	}
}
