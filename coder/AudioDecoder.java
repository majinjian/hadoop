package com.supinfo.transcode.coder;

import com.xuggle.xuggler.IAudioSamples;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IStreamCoder;

/*
 * class decode video into xuggler audio samples
 */
public class AudioDecoder {

	private IContainer container;
	private IStreamCoder audio_decoder;
	private int streamIndex;
	
	public AudioDecoder(IContainer container, int index){
		this.container = container;
		streamIndex = index;
		Initialize();
		audio_decoder.open(null,null);
	}
	
	private void Initialize(){
		audio_decoder = container.getStream(streamIndex).getStreamCoder();		
		if(audio_decoder == null){
			throw new RuntimeException("audio decoder is null");
		}
	}
	
	public IAudioSamples decodeAudio(IPacket packet){
		//create xuggler audio samples to take decoded audio samples, at least on sample
		IAudioSamples asamples = IAudioSamples.make(1, audio_decoder.getChannels());
		
		//decode the packet into xuggler audio samples until audio samples complete
		int decode_size;
		int offset = 0;
		while(offset < packet.getSize()){
			//decode function will override orignial data inside xuggler audiosamples when call it repeatly
			decode_size = audio_decoder.decodeAudio(asamples, packet, 0);
			//if decode failed it returns negative
			if(decode_size < 0){
				throw new RuntimeException("decode packet failed");
			}
			offset += decode_size;
		}
		return asamples;
	}
	
	public AudioEncoder getAudioEncoder(boolean keepDecoder){
		if(!keepDecoder){
			onclose();
		}
		return new AudioEncoder(audio_decoder.getChannels(), audio_decoder.getSampleRate());
	}
	
	public void onclose(){
		if(audio_decoder.close() < 0){
			throw new RuntimeException("close audio stream decoder failed");
		}
		audio_decoder.delete();
	}
}
