package com.supinfo.transcode.coder;

import com.supinfo.transcode.auxobject.TimeBaseFactory;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.ICodec.ID;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IPixelFormat;
import com.xuggle.xuggler.IRational;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.IStreamCoder.Direction;
import com.xuggle.xuggler.IStreamCoder.Flags;
import com.xuggle.xuggler.IVideoPicture;

/*
 * class to encode xuggler picture to new packet
 */
public class VideoEncoder {
	//the new video codec
	private static ICodec VIDEO_CODEC;
	
	

	private static final IPixelFormat.Type DEFAULT_PIXEL_FORMAT = IPixelFormat.Type.YUV420P; 
	
	
	private IStreamCoder video_encoder;
	private IRational timeBase;
	//the frame rate of old media video stream
	private IRational source_frameRate;
	private int width;
	private int height;
	
	public VideoEncoder(String format, int width, int height, IRational framerate){
		getCodec(format);
		this.width = width;
		this.height = height;
		source_frameRate = framerate;
		Initialize();
	}
	
	private void getCodec(String format){
		if(format.equals("flv")){
			VIDEO_CODEC = ICodec.findEncodingCodec(ID.CODEC_ID_FLV1);
		}
		else if(format.equals("mov") || format.equals("mp4")){
			VIDEO_CODEC = ICodec.findEncodingCodec(ID.CODEC_ID_H264);
		}
		else{
			VIDEO_CODEC = ICodec.findEncodingCodec(ID.CODEC_ID_MPEG4);
		}
	}
	
	private void Initialize(){
		video_encoder = IStreamCoder.make(Direction.ENCODING, VIDEO_CODEC);
		
		//set the time base
		timeBase = TimeBaseFactory.setTimeBase(VIDEO_CODEC, source_frameRate);
		
		video_encoder.setPixelType(DEFAULT_PIXEL_FORMAT);
		
		video_encoder.setTimeBase(timeBase);
		
		video_encoder.setWidth(width);
		
		video_encoder.setHeight(height);
		
		video_encoder.setFlag(Flags.FLAG_QSCALE, true);
		
		video_encoder.open(null, null);
	}
	
	public IPacket encodeVideo(IVideoPicture pic){
		//create new xuggler packet to take new ecoded data
		IPacket packet = IPacket.make();
		//if we decode succefully, xuggler picture should complete
		if(pic.isComplete()){
			/* 
			 * encode the xuggler picture to new packet with new codec 
			 */		
			int encode_size;
			if(!packet.isComplete()){
				//encode xuggler picture with suggested buffer size to allocate by xuggler
				encode_size = video_encoder.encodeVideo(packet, pic, 0);
				//if encode failed it returns negative
				if(encode_size < 0){
					throw new RuntimeException("encode packet failed");
				}
			}
		}
		return packet;
	}
	
	public IStreamCoder getCoder(){
		return video_encoder;
	}
	
	public void flush(IContainer container){
		if(video_encoder != null){
			IPacket packet = IPacket.make();
	        while (video_encoder.encodeAudio(packet, null, 0) >= 0 && packet.isComplete())
	        {
	          container.writePacket(packet);
	          packet.delete();
	          packet = IPacket.make();
	        }
	        packet.delete();
		}
	}
	
	public void onclose(){
		if(video_encoder.close() < 0){
			throw new RuntimeException("close video stream encoder failed");
		}
		video_encoder.delete();
	}
}
