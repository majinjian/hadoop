package com.supinfo.transcode.coder;

import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IStreamCoder;

import java.util.ArrayList;

import com.xuggle.xuggler.IAudioSamples;
import com.xuggle.xuggler.IAudioSamples.Format;
import com.xuggle.xuggler.IStreamCoder.Direction;

/*encode xuggler audio samples into packet*/
public class AudioEncoder {
	//the new audio codec
	private static final ICodec AUDIO_CODEC = ICodec.findEncodingCodec(ICodec.ID.CODEC_ID_MP3);
	
	private static final Format DEFAULT_SAMPLE_FORMAT = Format.FMT_S16;
	
	private IStreamCoder audio_encoder;
	
	private int channels;
	
	private int samepleRate;
	
	public AudioEncoder(int channels, int sampleRate){
		this.channels = channels;
		this.samepleRate = sampleRate;
		Initialize();
	}
	
	private void Initialize(){
		audio_encoder = IStreamCoder.make(Direction.ENCODING,  AUDIO_CODEC);
		
		audio_encoder.setSampleFormat(DEFAULT_SAMPLE_FORMAT);

		audio_encoder.setChannels(channels);

		audio_encoder.setSampleRate(samepleRate);
		
		audio_encoder.open(null,null);
	}
	
	public ArrayList<IPacket> encodeAudio(IAudioSamples asamples){
		
		ArrayList<IPacket> packets = new ArrayList<IPacket>();
		//if we decode succesfully, xuggler audio samples should complete
		if(asamples.isComplete()){
			
			//the number of decoded samples
			long number_samples = asamples.getNumSamples();
			long consumed_samples;
			//encode all decoded the samples
			for(consumed_samples = 0; consumed_samples < number_samples; /*add in loop*/){						
				
				//create new xuggler packet to take new ecoded data
				IPacket packet = IPacket.make();
				
				//encode audio samples from the sample we last consumed		 
				int result = audio_encoder.encodeAudio(packet, asamples, consumed_samples);
				consumed_samples += result;
				
				//if encode failed it returns negative
				if(result < 0){
					throw new RuntimeException("encode packet failed");
				}
				
				if(packet.isComplete()){
					packets.add(packet);
				}
			}
		}
		return packets;
	}
	
	public IStreamCoder getCoder(){
		return audio_encoder;
	}
	
	public void flush(IContainer container){
		if(audio_encoder != null){
			IPacket packet = IPacket.make();
			while (audio_encoder.encodeAudio(packet, null, 0) >= 0 && packet.isComplete())
	        {
	          container.writePacket(packet);
	          packet.delete();
	          packet = IPacket.make();
	        }
	        packet.delete();
		}
	}
	
	public void onclose(){
		if(audio_encoder.close() < 0){
			throw new RuntimeException("close audio stream encoder failed");
		}
		audio_encoder.delete();
	}
}
