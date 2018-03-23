package com.supinfo.transcode.streamhandler;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.xuggle.xuggler.io.IURLProtocolHandler;

/*
 * xuggler input stream handler 
 * to deal with FSDatainputStream for 
 * xuggler media container
 */
public class HDFSInputHandler implements IURLProtocolHandler {
	
	private FSDataInputStream fileIn;
	private FileSystem fs;
	private Path media_source;
	
	private static final Logger LOG = Logger.getLogger("info inspect");
	
	public HDFSInputHandler(Configuration conf, FileSplit split) 
			throws IOException {
		LOG.setLevel(Level.INFO);
		Initializer(conf, split, null);
		LOG.info("start xuggler inputstream handler");
	}
	
	public HDFSInputHandler(Configuration conf, Path path) 
			throws IOException{
		LOG.setLevel(Level.INFO);
		Initializer(conf, null, path);
		LOG.info("start xuggler inputstream handler");
	}
	
	/*
	 * method used to initialize variables
	 */
	private void Initializer(Configuration conf, FileSplit split, Path path) 
			throws IOException{
		
		if(split != null){
			media_source = split.getPath();
		}
		if(path != null){
			media_source = path;
		}
		fs = media_source.getFileSystem(conf);
		fileIn = fs.open(media_source);
	}
	
	/*
	 * called when container closed
	 */
	@Override
	public int close() {
		try{
			fileIn.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return 0;
	}
	
	@Override
	public boolean isStreamed(String arg0, int arg1) {
		return false;
	}
	
	/*
	 * open the stream
	 * normally in this program
	 * we don't need to care about it
	 */
	@Override
	public int open(String uri, int flag) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public int read(byte[] buffer, int size) {
		int readSize = 0;
		
		try{
			readSize = fileIn.read(buffer, 0, size);
			
			 /*
			  * IUrlProtocolHandler wants return value 
			  * to be zero if end of file
			  * is reached
			  */
			if(readSize == -1){
				readSize = 0;
			}
			
		}catch(Exception e){
			e.printStackTrace();
			//return -1 means error
			readSize = -1;
		}
		
		return readSize;
	}
	
	@Override
	public long seek(long offset, int whence) {
		
		long pos = 0;
		
		try{
			//get the size of source media
			FileStatus status = fs.getFileStatus(media_source);
            long len = status.getLen();
            
			switch(whence){
				//seek relative to current position
				case SEEK_CUR:
		        	LOG.info("seek current: " + offset);
		            break;
		        //seek relative relative to end
		        case SEEK_END:
		            LOG.info("seek end: " + offset);
		            break;
		    	//normally xuggler container only need to know size of source
		        case SEEK_SIZE:
		            pos = len;
		            LOG.info("seek size: " + pos);
		            break;
		        //seek to one position
		        case SEEK_SET:
		        default:
		            LOG.info("set seek: " + offset);
		            fileIn.seek( offset ); 
	                pos = fileIn.getPos();
		            break;
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return pos;
	}
	
	/*
	 * for input stream we don't need to write
	 */
	@Override
	public int write(byte[] buffer, int size) {
		return 0;
	}
}
