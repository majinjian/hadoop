package com.supinfo.transcode.auxobject;

import com.supinfo.transcode.coder.AudioDecoder;
import com.supinfo.transcode.coder.VideoDecoder;
import com.supinfo.transcode.streamhandler.HDFSInputHandler;
import com.xuggle.xuggler.ICodec.Type;
import com.xuggle.xuggler.IContainer;

/*
 * class represent the source media file
 */
public class SourceContainer {
	
	//Xuggler container of media source used to decode source media
	private static IContainer decode_container;
	
	private int streamNbr = -1;
	
	private static String new_format; 
	
	
	public int initialize(HDFSInputHandler handler, String format){
		
		new_format = format;
		
		decode_container = IContainer.make();
		
		//open the container from datainput stream
		//the format of which can be automatic detected
		decode_container.open(handler,IContainer.Type.READ, null);
		
		if(decode_container == null){
			throw new RuntimeException("no container");
		}
		
		streamNbr = decode_container.getNumStreams();
	
		if(streamNbr <= 0){
			throw new RuntimeException("bad file");
		}
		
		return streamNbr;
	}
	
	public Type getCodecType(int streamIndex){
		return decode_container.getStream(streamIndex).getStreamCoder().getCodecType();
	}
	
	public VideoDecoder getVideoDecoder(int streamIndex){		
		return new VideoDecoder(decode_container, streamIndex, new_format);
	}
	
	public AudioDecoder getAudioDecoder(int streamIndex){
		return new AudioDecoder(decode_container, streamIndex);
	}
	
	public void onclose(){
		if(decode_container.close() < 0){
			throw new RuntimeException("close source container failed");
		}
		decode_container.delete();
	}
}
