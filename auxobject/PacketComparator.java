package com.supinfo.transcode.auxobject;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * method used by this program to compare 
 * two xuggler packets information
 * in order to send value from mapper 
 * in ascending order
 */
public class PacketComparator extends WritableComparator{
	
	public PacketComparator() {
		super(PacketInfo.class, true);
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public int compare( WritableComparable obj1, WritableComparable obj2){
		PacketInfo p1 = (PacketInfo) obj1;
		PacketInfo p2 = (PacketInfo) obj2;
		return p1.compareTo(p2);
	}
}
