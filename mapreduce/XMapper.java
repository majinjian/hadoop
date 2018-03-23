package com.supinfo.transcode.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.supinfo.transcode.auxobject.DataGarbage;
import com.supinfo.transcode.auxobject.MapperKeyHandler;
import com.supinfo.transcode.auxobject.PacketInfo;
import com.supinfo.transcode.coder.AudioDecoder;
import com.supinfo.transcode.coder.AudioEncoder;
import com.supinfo.transcode.coder.CoderRetriever;
import com.supinfo.transcode.coder.VideoDecoder;
import com.supinfo.transcode.coder.VideoEncoder;
import com.supinfo.transcode.streamhandler.HDFSInputHandler;
import com.xuggle.ferry.IBuffer;
import com.xuggle.xuggler.IAudioSamples;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IVideoPicture;

/*
 * Mapper class for map job
 * the main class for decoding and encoding media packet
 */
public class XMapper extends Mapper<
								Text, 
								BytesWritable, 
								PacketInfo, 
								BytesWritable
								>{		
	private CoderRetriever coder_retriever;
	
	private VideoDecoder videoDecoder;	
	private VideoEncoder videoEncoder;
	
	private AudioDecoder audioDecoder;
	private AudioEncoder audioEncoder;
	
	private String new_format;
			
	//the byte array for incoming value
	private byte[] buffer;
	private byte[] encoded_bytes;
	private int size;
	//the xuggler buffer for raw value bytes
	private IBuffer xbuffer;
	private ByteBuffer encoded_buffer;

	private IPacket old_packet;
	private IPacket new_packet;
	
	//the incoming key string
	private String kstring;
	private String[] split;
	
	//packet type
	private String type;
	private String timestamp;


	private static final String VIDEO = "V";
	private static final String AUDIO = "A";
	
	private PacketInfo outkey;
	private BytesWritable outval;
	
	private static final Logger LOG = Logger.getLogger("info inspect");
	
		
	@Override
	protected void setup(Context context) throws IOException{
		//set the logger
		LOG.setLevel(Level.INFO);		
		//to initialize variables
		retrieve_coderRetriever(context);
		Initializer();
		//log the info
		LOG.info("mapper ready");		
	}
	
	/*
	 * method used to set corders
	 */
	private void Initializer(){
		if(!new_format.equals("mp3")){
			videoDecoder = coder_retriever.getVideoDecoder();
			videoEncoder = coder_retriever.getVideoEncoder(true);
		}
		audioDecoder = coder_retriever.getAudioDecoder();
		audioEncoder = coder_retriever.getAudioEncoder(true);
	}
		
	/*
	 * Mapper job method
	 * used to receive packets and keys
	 * from XRecordreader
	 */
	public void map(Text key, BytesWritable value, Context context) 
			throws IOException, InterruptedException {
		
		splitKey(key);
		//convert value to byte array
		getPacket(value);
		decode_encode_packet(context);
	}
	
	/*
	 * method to decode and encode media
	 */
	private void decode_encode_packet(Context context) 
			throws IOException, InterruptedException{
		//if it is a video packet
		if(type.equals(VIDEO) && !new_format.equals("mp3")){
			videoHandler(context);
		}
		//if it is a audio packet
		if(type.equals(AUDIO)){
			audioHandler(context);
		
		}
	}
	
	private void videoHandler(Context context) 
			throws IOException, InterruptedException{
		//decode video packet into picture			
		IVideoPicture pic = videoDecoder.decodeVideo(old_packet);
		DataGarbage.clearData(old_packet, xbuffer, null, null);
		
		//encode video
		new_packet = videoEncoder.encodeVideo(pic);
		
		//if we get a complete packet, send the key/value to shuffle
		if(new_packet.isComplete()){
			sendResult(context);
		}			
		DataGarbage.clearData(new_packet, null, pic, null);
	}
	
	private void audioHandler(Context context) 
			throws IOException, InterruptedException{
		//decode audio into samples
		IAudioSamples asamples = audioDecoder.decodeAudio(old_packet);
			
		DataGarbage.clearData(old_packet, xbuffer, null, null);
		
		//encode audio	
		ArrayList<IPacket> audio_packets = audioEncoder.encodeAudio(asamples);
		
		//send each packet audiosamples to shuffle
		for(IPacket packet: audio_packets){
			new_packet = packet;
			sendResult(context);
			DataGarbage.clearData(packet, null, null, null);
		}
		audio_packets.clear();
		audio_packets = null;
		DataGarbage.clearData(null, null, null, asamples);
	}
	
	private void splitKey(Text key){
		kstring = key.toString();
		split = kstring.split(" ");
		
		//get the paket type
		type = split[0];
		timestamp = split[1];
	}
	
	private void getPacket(BytesWritable value){
		buffer = value.copyBytes();
		size = buffer.length;
		//create xuggler buffer from raw bytes
		xbuffer = IBuffer.make(null, buffer, 0, size);
		buffer = null;
		
		//create xuggler packet from xuggler buffer
		old_packet = IPacket.make(xbuffer);
		old_packet.setComplete(true, size);
		old_packet.setTimeStamp(new Long(timestamp));
	}
	
	/*
	 * the method used to convert packet metadata to bytes
	 * as key to output
	 */
	private void setKey(IPacket packet) throws IOException{
		
		PacketInfo packetInfo = new PacketInfo();		
		
		//set the stream of packet
		if(type.equals(VIDEO)){
			packetInfo.setType(VIDEO);
		}else {
			packetInfo.setType(AUDIO);
		}		
		
		//set the attributes of packet info instance
		MapperKeyHandler.setMapperKey(packetInfo, new_packet);
		
		outkey = packetInfo;
	}
	
	/*
	 * the method used to send key/value pairs to shuffle
	 */
	private void sendResult(Context context) 
			throws IOException, InterruptedException{
		
		//get encoded byte buffer from encoded packet
		encoded_buffer = new_packet.getByteBuffer();
		//convert encoded buffer to byte array
		encoded_bytes = new byte[encoded_buffer.remaining()];
		encoded_buffer.get(encoded_bytes);
		
		//create value output from encoded buffer
		outval = new BytesWritable(encoded_bytes);
		setKey(new_packet);

		encoded_buffer = null;
	
		context.write(outkey, outval);
	}	
	
	private void retrieve_coderRetriever(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		//to know if the destination format is mp3
		new_format = conf.get("format");
		InputSplit inputsplit = context.getInputSplit();
		FileSplit split = (FileSplit) inputsplit;
		HDFSInputHandler handler = new HDFSInputHandler(conf, split);
		coder_retriever = new CoderRetriever(handler, new_format);
	}
	
	@Override
	protected void cleanup(Context context){
		if(videoDecoder != null){
			videoDecoder.onclose();
			videoEncoder.onclose();
		}
		if(audioDecoder != null){
			audioDecoder.onclose();
			audioEncoder.onclose();
		}
		coder_retriever.onclose();
	}
}
