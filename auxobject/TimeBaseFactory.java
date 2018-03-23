package com.supinfo.transcode.auxobject;

import java.util.List;

import com.xuggle.xuggler.Global;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IRational;

public class TimeBaseFactory {
	
	private static final IRational DEFAULT_TIMEBASE = IRational.make(
		    1, (int)Global.DEFAULT_PTS_PER_SECOND);
	
	/*
	 * the method used to set proper time base 
	 * which based on frame rate
	 * for video stream
	 */
	public static IRational setTimeBase(ICodec codec, IRational frameRate){
		// the time base to set
		IRational timebase = null;
		// get the list of supproted frame rate of encoding codec
		List<IRational> supportedFrameRates = codec.getSupportedVideoFrameRates();
		
		/*
		 * if we can retrieve supported frame rate from xuggler
		 * we set time base based on frame rate
		 */
		if (supportedFrameRates != null && supportedFrameRates.size() > 0){
			//the best possible frame rate
			IRational highestResolution = null;
			
			// If we have a list of supported frame rates, then
	        // we must pick at least one of them.  and if the
	        // user passed in a frameRate, it must match 
	        // this list.
			
			for(IRational supportedRate: supportedFrameRates){
				//if the supporte framerate is negative
				if (!IRational.positive(supportedRate)){
					//retireve next supported frame rate
					continue;
				}
				//set the best frame rate from supported frame rate
				if (highestResolution == null){
					highestResolution = supportedRate.copyReference();
				}
				//if the frame rate we provided is positive
				if (IRational.positive(frameRate)){
					//if frame rate is equal to supported frame rate, use this
					if (supportedRate.compareTo(frameRate) == 0){
						highestResolution = frameRate.copyReference();
					}
					//is supported frame rate less than frame rate provided, used provided frame rate
					else if (highestResolution.getDouble() < supportedRate.getDouble()){
						//clear best frame rate
						highestResolution.delete();
			            highestResolution = supportedRate.copyReference();
					}
					//clear supported frame rate to take next
					supportedRate.delete();
				}
			}
			
			// if we had a frame rate suggested, but we
	        // didn't find a match among the supported elements,
	        // throw an error.
	        if (IRational.positive(frameRate) &&
	            (highestResolution == null ||
	                highestResolution.compareTo(frameRate) != 0)){
	        	
	        	throw new UnsupportedOperationException("container does not"+
	  	              " support encoding at given frame rate: " + frameRate);
	        }
	          
			
	        // if we got through the supported list and found NO valid
	        // resolution, fail.
	        if (highestResolution == null){
	        	throw new UnsupportedOperationException(
	  	              "could not find supported frame rate for container");
	        }
	         
	        //set the time base based on best frame rate
	        if (timebase == null){
	        	timebase = IRational.make(highestResolution.getDenominator(),
	  	              highestResolution.getNumerator());
	        }
	         
	        //clear the temp used best frame rate
	        highestResolution.delete();
	        highestResolution = null;
		}
		
		
		// if a positive frame rate was passed in 
		// and no supported frame rate, we
	    // should either use the inverse of it
		if (IRational.positive(frameRate) && timebase == null){
			timebase = IRational.make(
	            frameRate.getDenominator(),
	            frameRate.getNumerator());
	    }
		
		// if all the possible condition not match
		// we have to set time base as our default
		// time base
		if (timebase == null){
			timebase = DEFAULT_TIMEBASE;
	        
	        // Finally MPEG4 has some code failing if the time base
	        // is too aggressive...
	        if (codec.getID() == ICodec.ID.CODEC_ID_MPEG4 &&
	        	timebase.getDenominator() > ((1<<16)-1)){
	        	// this codec can't support that high of a frame rate
	        	timebase.delete();
	        	timebase = IRational.make(1,(1<<16)-1);
	        }
	        
		}
		return timebase;
	}
}
