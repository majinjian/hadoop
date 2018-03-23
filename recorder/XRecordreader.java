package com.supinfo.transcode.recorder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IStreamCoder;
import com.supinfo.transcode.streamhandler.HDFSInputHandler;
import com.xuggle.xuggler.ICodec.Type;

/*
 * Custom record reader is
 * used to read meida stream packet
 * by packet on hdfs file split via
 * xuggler
 */
public class XRecordreader extends RecordReader<Text, BytesWritable>{

	private Text key;	
	private BytesWritable value = new BytesWritable();
	
	//the start of the split (offset)
	private long start;
	private long end;
	private long pos = 0;
	
	//bytebuffer used to store bytes from xuggler buffer
	private ByteBuffer buffer;
	private int buffer_size;
	private byte[] buffer_bytes;
	
	//xuggler container
	private IContainer container;
	private	int streamNbr = -1;
	private IStreamCoder coder;
	//xuggler media stream packet
	private IPacket packet = null;
	private int streamIndex;
	private long timestamp;
	
	
	//set the loop time for debug
	private int loop = 0;
	private static final Logger LOG = Logger.getLogger("info inspect");
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		LOG.setLevel(Level.INFO);	
		LOG.info("start X record reader");
		
		Configuration conf = context.getConfiguration();	
		Initializer(conf, split);
		
		LOG.info("split reading started");
	}
	
	/*
	 * method used to set variable and initialize XRecord Reader
	 */
	private void Initializer(Configuration conf, InputSplit inputsplit){
		
		FileSplit split = (FileSplit)inputsplit;
		
		start = split.getStart();
		
		end = start + split.getLength();
		
		//now we get the container, we need to analyze it
		
		try {		
			HDFSInputHandler handler = new HDFSInputHandler(conf, split);
			container = IContainer.make();
			container.open(handler,IContainer.Type.READ, null);
			
			LOG.info("container opened");
			
			streamNbr = container.getNumStreams();
			
			//if stream less than 0, we cannot handle this media file
			if(streamNbr <= 0){
				throw new RuntimeException("bad file");
			}
			
			this.AdjustPos();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*
	 * adjust the position for where
	 * we read the split, because file
	 * split is divided by HDFS block not
	 * packet
	 */
	private void AdjustPos() throws IOException{
		//the split not start from beginning
		
		if(start !=0 ){
			/*
			 * we need to kown the end
			 * position read by last container
			 * in this split in order to reading 
			 * the new packet which may not start from
			 * the begin of this split
			 */
			LOG.info("adjusting reading position");
			
			IPacket temp_packet = IPacket.make();
			
			//read packet until position within this split 
			while(pos < start){
				container.readNextPacket(temp_packet);
				pos = temp_packet.getPosition();
			}
			/*
			 * now packet position should >= start
			 * we actually read packet of this split from
			 * this packet position. 
			 * let start = packet position
			 * also, this packet needed to be read
			 * so record this packet
			 */
			start = pos;
			packet = temp_packet;
		}
		pos = start;
	}
	
	/*
	 * we close the FSDataInputStream in
	 * HDFSInputHandler, no need here
	 */
	@Override
	public void close() throws IOException {
		/*
		 * see HDFSInputHandler close()
		 */
	}
	
	@Override
	public float getProgress() throws IOException {
		if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
	}
	
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}
	
	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}
	
	
	/*
	 * if 
	 * this file split
	 * start from middle of source media
	 * our first packet has already been
	 * stored into variable packet in Adjust(), 
	 * so direct use it
	 * else
	 * we read next packet via xuggler container
	 * into variable packet
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		if(packet == null){
			packet = IPacket.make();
			if(container.readNextPacket(packet) < 0){
				key = null;
				value = null;
				return false;
			}			
			pos = packet.getPosition();
		}
		
		if(pos < end){
			//we need to know which stream the packet belong to
			streamIndex = packet.getStreamIndex();
			coder = container.getStream(streamIndex).getStreamCoder();
			timestamp = packet.getTimeStamp();
			
			//create the value of key
			
			if(coder.getCodecType() == Type.CODEC_TYPE_VIDEO){			
				 //"V" represent video stream
				key = new Text("V " + Long.toString(timestamp));
				
				LOG.info("reading stream: Video at loop: " + loop);
			}
			
			if(coder.getCodecType() == Type.CODEC_TYPE_AUDIO){
				 //"A" represent audio stream
				key = new Text("A " + Long.toString(timestamp));
				LOG.info("reading stream: Audio at loop: " + loop);
			}
	
			//convert the xuggler packet into bytes then give to mapper
			 
			buffer = packet.getByteBuffer();
			buffer_size = buffer.remaining();
			buffer_bytes = new byte[buffer_size];
			//fill the byte array
			buffer.get(buffer_bytes);
			
			value.set(buffer_bytes, 0, buffer_bytes.length);

			packet = null;
			LOG.info("read position: " + pos);
			
			loop++;
			
			//return key/value to map
			return true;
			
		}else{
			//reach the end return nothing
			key = null;
			value = null;
			return false;
		}
	}
}
