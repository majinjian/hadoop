package com.supinfo.transcode.ioformat;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.supinfo.transcode.recorder.XRecordreader;


public class MediaInputFormat extends FileInputFormat<Text, BytesWritable>{
	//create log
	private static final Logger LOG = Logger.getLogger("info inspect");
	


	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		//set the logger
		LOG.setLevel(Level.INFO);
		//log info
		LOG.info("create X Record Reader");
				
		return new XRecordreader();
	}
}
