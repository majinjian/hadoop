package com.supinfo.transcode.auxobject;

import com.xuggle.ferry.IBuffer;
import com.xuggle.xuggler.IAudioSamples;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IVideoPicture;

public class DataGarbage {
	
	/*
	 * the method used to clear data we never use again
	 */
	public static void clearData(IPacket packet, IBuffer xbuffer, IVideoPicture pic, IAudioSamples samples){
		//delete the old xuggler packet
		if(packet != null){
			packet.delete();
		}
		//delete the old xuggler buffer
		if(xbuffer != null){
			xbuffer.delete();
		}
		//delete the od xuggler video picture
		if(pic != null){
			pic.delete();
		}
		//delete the old xuggler audio samples
		if(samples != null){
			samples.delete();
		}
	}
	
}
