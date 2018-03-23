package com.supinfo.transcode.recorder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.supinfo.transcode.auxobject.MapperKeyHandler;
import com.supinfo.transcode.auxobject.PacketInfo;
import com.supinfo.transcode.coder.AudioEncoder;
import com.supinfo.transcode.coder.CoderRetriever;
import com.supinfo.transcode.coder.VideoEncoder;
import com.supinfo.transcode.streamhandler.HDFSInputHandler;
import com.xuggle.ferry.IBuffer;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IPacket;


public class XRecordwriter extends RecordWriter<PacketInfo, BytesWritable>{
	
	private CoderRetriever coder_retriever;
	private VideoEncoder videoEncoder = null;
	private AudioEncoder audioEncoder = null;
	
	private String new_format;
	//the container of transcoded media
	private IContainer container;
	
	private String packet_type;
	
	private static final String VIDEO = "V";
	private static final String AUDIO = "A";
	
	private static Integer video_index = null;
	private static Integer audio_index = null;
	
	private static final String HDFS_PREFIX = "hdfs://master:9000/user/hadoop";
	private static String inputPath = "/";
	private static String inputFile = "/";
	private static String outputPath = "/";
	private static String localOutputFile = "/home/hadoop/transcode-videos/";
	private static String outputFile = "/";
	
	private static final Logger LOG = Logger.getLogger("info inspect");
	
	public XRecordwriter(TaskAttemptContext context) throws IOException {
		LOG.setLevel(Level.INFO);
		try {
			Configuration conf = context.getConfiguration();
			Initializer(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOG.info("start X Record Writer");
	}
	
	/*
	 * method used to initialize variable for class
	 */
	private void Initializer(Configuration conf) throws IOException{
		localOutputFile += conf.get("outputFile");
		inputPath += conf.get("inputPath");
		inputFile += conf.get("inputFile");
		new_format = conf.get("format");
		coder_retriever = retrieve_coderRetriever(conf);
		
		//get encoder without decoder
		if(!new_format.equals("mp3")){
			videoEncoder = coder_retriever.getVideoEncoder(false);
		}
		audioEncoder = coder_retriever.getAudioEncoder(false);
		
		container = IContainer.make();
		container.open(localOutputFile, IContainer.Type.WRITE, null);
		if(videoEncoder != null){
			container.addNewStream(videoEncoder.getCoder());
			video_index = 0;
		}
		if(audioEncoder != null){
			container.addNewStream(audioEncoder.getCoder());
			if(video_index != null){
				audio_index = 1;
			}
			else{
				audio_index = 0;
			}
		}
		
		
		if(container.writeHeader() < 0){
			throw new RuntimeException("cannot write header");
		}
	}
	
	
	private CoderRetriever retrieve_coderRetriever (Configuration conf) 
			throws IOException{
		Path source_path = new Path(
				HDFS_PREFIX +
				inputPath +
				inputFile
				);
		HDFSInputHandler handler = new HDFSInputHandler(conf, source_path);
		return new CoderRetriever(handler, new_format);
	}
	
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		finishMedia();
		LOG.info("transcode finished");
		Configuration conf = context.getConfiguration();
		upload(conf);
		deleteLocalFile();
		LOG.info("job finised");
	}

	@Override
	public void write(PacketInfo key, BytesWritable value) throws IOException, InterruptedException {
		byte[] value_bytes = value.copyBytes();
		//create xuggler buffer from value bytes
		IBuffer xbuffer = IBuffer.make(null, value_bytes, 0, value_bytes.length);
		
		IPacket packet = IPacket.make(xbuffer);
		packet_type = MapperKeyHandler.setPacket(key, packet);
		
		if(xbuffer != null){
			xbuffer.delete();
			xbuffer = null;
		}
		
		if(packet_type.equals(VIDEO)){		
			packet.setStreamIndex(video_index);
		}
		
		if(packet_type.equals(AUDIO)){
			packet.setStreamIndex(audio_index);
		}
		
		
		//write packet into new file
		try{
			if(container.writePacket(packet) < 0){
				LOG.info("writing error: " + packet.toString());
			}
		}finally{
			//clear the packet
			if(packet != null){
				packet.delete();
				packet = null;
			}
		}
	}
	
	private void finishMedia(){
		if(container.writeTrailer() < 0){
			throw new RuntimeException("write trail failed");
		}
		
		if(videoEncoder != null){
			videoEncoder.flush(container);
			videoEncoder.onclose();
		}
		if(audioEncoder != null){
			audioEncoder.flush(container);
			audioEncoder.onclose();
		}
		container.flushPackets();
		coder_retriever.onclose();
	}
	
	private void deleteLocalFile(){
		File file = new File(localOutputFile);
		if(file.exists()){
			file.delete();
		}
	}
	
	private void upload(Configuration conf) throws IOException{
		LOG.info("uploading transcoded file to HDFS");
		
		outputPath += conf.get("outputPath");
		outputFile += conf.get("outputFile");
		Path output_path = new Path(
				HDFS_PREFIX +
				outputPath +
				outputFile);
		 FileSystem fs = output_path.getFileSystem(conf);
		
		try {
			FileInputStream fileInputStream = new FileInputStream(localOutputFile);
			FSDataOutputStream fileOut = fs.create(output_path);
			//copy bytes from input stream to output stream, 4096 bytes at a time
			IOUtils.copyBytes(fileInputStream, fileOut, 4096, true);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
