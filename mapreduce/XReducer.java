package com.supinfo.transcode.mapreduce;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import com.supinfo.transcode.auxobject.PacketInfo;


public class XReducer extends Reducer<PacketInfo, 
  									  BytesWritable, 
  									  PacketInfo, 
  									  BytesWritable>{

	private static final Logger LOG = Logger.getLogger("info inspect");
	
	public XReducer() {
		LOG.setLevel(Level.INFO);
		LOG.info("reducer ready");
	}						 

	/*
	 * the reducer class
	 * used to directly pass the handled data
	 * to record writer
	 * since we have already reencoded our
	 * media packet
	 */
	
	public void reduce(PacketInfo key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException{
		
		//directly send the shuffled key/value to X record writer
		for(BytesWritable val : values){
			context.write(key, val);
		}
	}
}
