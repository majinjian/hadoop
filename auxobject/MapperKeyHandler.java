package com.supinfo.transcode.auxobject;

import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IRational;

public class MapperKeyHandler {
	
	private static int timebase_Numerator;
	private static int timebase_Denominator;
	
	public static void setMapperKey(PacketInfo packetInfo, IPacket packet){
		//set the attributes of packet info instance
		packetInfo.setPts(packet.getPts());
		packetInfo.setDts(packet.getDts());
		packetInfo.setDuration(packet.getDuration());
		packetInfo.setFlags(packet.getFlags());
		packetInfo.setKey(packet.isKeyPacket());
		packetInfo.setTimeBaseNumerator(packet.getTimeBase().getNumerator());
		packetInfo.setTimeBaseDenominator(packet.getTimeBase().getDenominator());
		packetInfo.generateID();
	}
	
	public static String setPacket(PacketInfo packetInfo, IPacket packet){
		
		packet.setDts(packetInfo.getDts());
		packet.setPts(packetInfo.getPts());
		packet.setDuration(packetInfo.getDuration());
		packet.setFlags(packetInfo.getFlags());
		packet.setKeyPacket(packetInfo.isKeyPacket());
		timebase_Numerator = packetInfo.getTimeBaseNumerator();
		timebase_Denominator = packetInfo.getTimeBaseDenominator();
		packet.setTimeBase(IRational.make(timebase_Numerator, timebase_Denominator));
		return packetInfo.getType();
	}
}
