package com.chinamobile.bcbsp.examples.subgraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.comm.IMessage;

//import com.chinamobile.bcbsp.newversion.comm.IMessage;

public class Sub_Message implements IMessage<Integer, String, Integer> {

	private int messageId;
	private String value;
	public static final Log LOG = LogFactory.getLog(Sub_Message.class);
	@Override
	public void write(DataOutput out) throws IOException {
		//LOG.info("Sub_Message  write    is    used");
		out.writeInt(messageId);
		out.writeInt(value.length());
		char[] c = value.toCharArray();
		for (int i = 0; i < value.length(); i++) {
			out.writeChar(c[i]);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		//LOG.info("Sub_Message  readFields   is    used");
		this.messageId = in.readInt();
		int length = in.readInt();
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < length; i++) {
			sb.append(in.readChar());
		}
		this.value = sb.toString();

	}
	@Override
	public int getDstPartition() {
		return -1;
	}

	@Override
	public void setDstPartition(int partitionID) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getDstVertexID() {
		return String.valueOf(messageId);
	}

	@Override
	public Integer getMessageId() {
		return messageId;
	}

	@Override
	public void setMessageId(Integer id) {
		messageId = id;

	}

	@Override
	public Integer getTag() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setTag(Integer tag) {
		// TODO Auto-generated method stub

	}

	@Override
	public int getTagLen() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Integer getData() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getContent() {
		return value;
	}

	@Override
	public int getContentLen() {
		return value.length()*2;
	}

	@Override
	public void setContent(String content) {
		value = content;

	}

	@Override
	public String intoString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void fromString(String msgData) {
		// TODO Auto-generated method stub

	}

	// Note For Chen Chang Ning
	@Override
	public long size() {
		// TODO Auto-generated method stub
		//return 0;
//		added by songjianze
		long size=value.length()+Integer.SIZE/8;
		return size;
		
	}

	
	
	//NOTE ADD 20140329  
	
	/*public boolean combineIntoContainer(
			Map<Integer, ArrayList<IMessage<Integer, String, Integer>>> container) {
		ArrayList<IMessage<Integer, String, Integer>> tmp = container.get(messageId);
		if(tmp!=null){
//			LOG.info("before add data is  "+getContent());
//			print(tmp);
			tmp.add(this);
//			LOG.info("tmp!=null");
//			print(tmp);
			if(tmp.size()>0){
				return false;
			}	
			return true;
		}else{
			ArrayList<IMessage<Integer, String, Integer>> temp1 = new ArrayList<IMessage<Integer, String, Integer>>();
			temp1.add(this);
//			LOG.info("tmp=null");
//			print(temp1);
			container.put(messageId, temp1);
			return false;
		}
		 
		
	
	}
	*/
	void print(ArrayList<IMessage<Integer, String, Integer>> a){
		  for(IMessage<Integer, String, Integer> e:a){
			  LOG.info("ljn test : vertex ID is " + e.getMessageId()+"   data is +" + e.getContent());
		  }
		  
	  }

	@Override
	public boolean combineIntoContainer(
			Map<String, IMessage<Integer, String, Integer>> container) {
		// TODO Auto-generated method stub
		return false;
	}
	

}