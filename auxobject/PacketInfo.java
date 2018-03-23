package com.supinfo.transcode.auxobject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/*
 * the class represent a xuggler packet information 
 * that we will used it as key in
 * map reduce job.
 */
public class PacketInfo implements WritableComparable<PacketInfo>{
	
	private String ID = null;
	private Long dts = null;
	private long pts;
	private boolean isKey;
	private int flags;
	private int tb_Numerator;
	private int tb_Denominator;
	private long duration;
	private String type = null;
	
	public void setDts(long dts){
		this.dts = dts;
	}
	
	public void setPts(long pts){
		this.pts = pts;
	}
	
	public void setKey(boolean iskey){
		this.isKey = iskey;
	}
	
	public void setFlags(int flags){
		this.flags = flags;
	}
	
	public void setTimeBaseNumerator(int num){
		this.tb_Numerator = num;
	}
	
	public void setTimeBaseDenominator(int den){
		this.tb_Denominator = den;
	}
	
	public void setDuration(long duration){
		this.duration = duration;
	}
	
	public void setType(String type){
		this.type = type;
	}
	
	/* 
	 * we combine type and dts of packet as ID string
	 * in this way each packet will have unique ID.
	 * For example we have video type "V". and dts 10
	 * then the ID will be V10
	 * 
	 */
	public void generateID(){
		
		if(type != null && dts != null){
			ID = type + dts.toString();
		}
		else {
			throw new RuntimeException("no enough info to generate packetInfo ID");
		}
	}
	
	public long getDts(){
		return dts;
	}
	
	public long getPts(){
		return pts;
	}
	
	public boolean isKeyPacket(){
		return isKey;
	}
	
	public int getFlags(){
		return flags;
	}
	
	public int getTimeBaseNumerator(){
		return tb_Numerator;
	}
	
	public int getTimeBaseDenominator(){
		return tb_Denominator;
	}
	
	public long getDuration(){
		return duration;
	}
	
	public String getType(){
		return type;
	}
	
	public String getID(){
		return ID;
	}
	
	/*
	 * deseriablize object
	 */
	@Override
	public void readFields(DataInput inputStream) throws IOException {
		//retrieve attributes from inputstream
		ID = inputStream.readUTF();
		dts = inputStream.readLong();
		pts = inputStream.readLong();
		isKey = inputStream.readBoolean();
		flags = inputStream.readInt();
		tb_Numerator = inputStream.readInt();
		tb_Denominator = inputStream.readInt();
		duration = inputStream.readLong();
		type = inputStream.readUTF();
	}
	
	
	/*
	 * serialize object
	 */
	@Override
	public void write(DataOutput outputStream) throws IOException {
		//write attributes to outputstream
		outputStream.writeUTF(ID);
		outputStream.writeLong(dts);
		outputStream.writeLong(pts);
		outputStream.writeBoolean(isKey);
		outputStream.writeInt(flags);
		outputStream.writeInt(tb_Numerator);
		outputStream.writeInt(tb_Denominator);
		outputStream.writeLong(duration);
		outputStream.writeUTF(type);
	}
	
	/*
	 * method called by hadoop to compare two PacketInfo
	 * instance
	 */
	@Override
	public int compareTo(PacketInfo packetInfo) {
		//we compare dts of two instance
		long thisDts = this.dts;
		String thisID = this.ID;
		long thatDts = packetInfo.getDts();
		String thatID = packetInfo.getID();

		return ((thisDts < thatDts) ? -1 : (thisID == thatID) ? 0 : 1);
	}
	
	/*
	 * we generate hash code of id string as this instance hashcode
	 */
	@Override
	public int hashCode(){
		return ID.hashCode();	
	}
	
	
	/*
	 * if ID stream are same then packet info instances are same
	 */
	@Override
	public boolean equals(Object obj){
		PacketInfo packetInfo = (PacketInfo) obj;
		return ID.equals(packetInfo.getID());
	}
}
