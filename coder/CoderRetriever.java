package com.supinfo.transcode.coder;

import com.supinfo.transcode.auxobject.SourceContainer;
import com.supinfo.transcode.streamhandler.HDFSInputHandler;
import com.xuggle.xuggler.ICodec;

public class CoderRetriever {
	
	private SourceContainer source_container = new SourceContainer();
	
	private VideoDecoder videoDecoder;
	
	private AudioDecoder audioDecoder;
	
	private int streamNbr = -1;
	
	public CoderRetriever(HDFSInputHandler handler, String format){
		streamNbr = source_container.initialize(handler, format);
	}
	
	public VideoDecoder getVideoDecoder(){
		//loop the stream find decoder and create encoder for each stream
		for(int i = 0; i < streamNbr; i++){
			//set video decoder/encoder
			if(source_container.getCodecType(i) == ICodec.Type.CODEC_TYPE_VIDEO){
				videoDecoder = source_container.getVideoDecoder(i);
				return videoDecoder;
			}
		}	
		return null;
	}
	
	public VideoEncoder getVideoEncoder(boolean keepDecoder){
		if(videoDecoder != null){
			return videoDecoder.getVideoEncoder(keepDecoder);
		}
		else if(getVideoDecoder() != null){		
			return videoDecoder.getVideoEncoder(keepDecoder);
		}
		return null;
	}
	
	public AudioDecoder getAudioDecoder(){
		//loop the stream find decoder and create encoder for each stream
		for(int i = 0; i < streamNbr; i++){
			//set video decoder/encoder
			if(source_container.getCodecType(i) == ICodec.Type.CODEC_TYPE_AUDIO){
				audioDecoder = source_container.getAudioDecoder(i);
				return audioDecoder;
			}
		}	
		return null;
	}
	
	public AudioEncoder getAudioEncoder(boolean keepDecoder){
		if(audioDecoder != null){
			return audioDecoder.getAudioEncoder(keepDecoder);
		}
		else if(getAudioDecoder() != null){
			return audioDecoder.getAudioEncoder(keepDecoder);
		}
		return null;
	}
	
	public void onclose(){
		source_container.onclose();
	}
}
