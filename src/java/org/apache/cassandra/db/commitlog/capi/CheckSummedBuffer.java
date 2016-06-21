package org.apache.cassandra.db.commitlog.capi;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.research.capiblock.CapiBlockDevice;

public class CheckSummedBuffer {
	static final Logger logger = LoggerFactory.getLogger(CheckSummedBuffer.class);
	private final ByteBuffer buffer;
	private final BufferedDataOutputStreamPlus bufferStream;
	private final CRC32 checksum = new CRC32();
	
	public CheckSummedBuffer(){
		buffer = ByteBuffer.allocateDirect(DatabaseDescriptor.getCAPIFlashCommitLogBufferSizeInBlocks() * CapiBlockDevice.BLOCK_SIZE);
		bufferStream = new DataOutputBufferFixed(buffer);
	}

	public void clear() {
		checksum.reset();
		buffer.clear();		
	}
	
	public BufferedDataOutputStreamPlus getStream() {
		return bufferStream;
	}
	
	public Checksum getChecksum(){
		return checksum;
	}
	
	public ByteBuffer  getBuffer(){
		return buffer;
	}
	
	public Checksum calculateCRC(int offset,int length){
		checksum.reset();
        int position = buffer.position();
        int limit = buffer.limit();
        buffer.position(offset).limit(offset + length);
        checksum.update(buffer);
        buffer.position(position).limit(limit);
		return checksum;
	}

}
