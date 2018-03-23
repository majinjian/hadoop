package com.supinfo.transcode.coder;

import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.IVideoPicture;

/*
 * class decode video into xuggler picture
 */
public class VideoDecoder {
	
	private IContainer container;
	private IStreamCoder video_decoder;
	private int width = 0;
	private int height = 0;
	private int streamIndex;
	private String new_format;
	
	public VideoDecoder(IContainer container, int index, String format){
		this.container = container;
		streamIndex = index;
		new_format = format;
		Initialize();
	}
	
	private void Initialize(){
		video_decoder = container.getStream(streamIndex).getStreamCoder();
		
		if(video_decoder == null){
			throw new RuntimeException("video decoder is null");
		}		
		
		width = video_decoder.getWidth();
		height = video_decoder.getHeight();
		video_decoder.open(null, null);
	}
	
	public IVideoPicture decodeVideo(IPacket packet){
		//create a xuggler picture to take the decoded video picture
		IVideoPicture pic = IVideoPicture.make(
								video_decoder.getPixelType(), 
								width, 
								height);
		
		//decode the packet into xuggler picture, until picture is complete
		int decode_size;
		int offset = 0;
		while(offset < packet.getSize()){
			//decode function will override orignial data inside xuggler picture when call it repeatly
			decode_size = video_decoder.decodeVideo(pic, packet, offset);
			//if decode failed it returns negative
			if(decode_size < 0){
				throw new RuntimeException("decode packet failed");
			}
			offset += decode_size;
		}
		return pic;
	}
	
	public VideoEncoder getVideoEncoder(boolean keepDecoder){
		if(!keepDecoder){
			onclose();
		}
		return new VideoEncoder(new_format, width, height, video_decoder.getFrameRate());
	}
	
	public void onclose(){
		if(video_decoder.close() < 0){
			throw new RuntimeException("close audio stream decoder failed");
		}
		video_decoder.delete();
	}
}
