package com.supinfo.transcode.ioformat;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.supinfo.transcode.auxobject.PacketInfo;
import com.supinfo.transcode.recorder.XRecordwriter;


public class MediaOutputFormat extends FileOutputFormat<PacketInfo, BytesWritable>{
	
	private static final Logger LOG = Logger.getLogger("info inspect");

	@Override
	public RecordWriter<PacketInfo, BytesWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		LOG.setLevel(Level.INFO);	
		LOG.info("create X Record Writer");
		
		return new XRecordwriter(context);
	}
}
