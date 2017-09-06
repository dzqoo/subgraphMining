package com.chinamobile.bcbsp.examples.subgraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.chinamobile.bcbsp.api.Edge;

public class SMEdge extends Edge<Integer, String> {

	int vertexID = 0;
	char vertexValue = 0;
	char edgeValue = 0;

	String code;

	@Override
	public void fromString(String edgeData) throws Exception {
		String[] buffer = edgeData.split(Constantself.SPLIT_FLAGD);
		if(buffer.length!=3)
			throw new Exception();
		this.vertexID = Integer.valueOf(buffer[0]);
		this.vertexValue = Character.valueOf(buffer[1].charAt(0));
		this.edgeValue = Character.valueOf(buffer[2].charAt(0));
	}

	public void setVertexValue(char vertexValue) {
		this.vertexValue = vertexValue;
	}

	@Override
	public String getEdgeValue() {
		return this.vertexValue+Constantself.SPLIT_FLAGD +this.edgeValue;
		
	}

	@Override
	public Integer getVertexID() {
		return this.vertexID;
	}

	@Override
	public String intoString() {
		return this.vertexID + Constantself.SPLIT_FLAGD + this.vertexValue
				+ Constantself.SPLIT_FLAGD + this.edgeValue;
	}

	public void setEdgeValue(char arg0) {
		this.edgeValue = arg0;
	}

	@Override
	public void setVertexID(Integer arg0) {
		this.vertexID = arg0;
	}

	@Override
//	public void readFields(DataInput in) throws IOException {
//		this.vertexID = in.readInt();
//		String temp=in.readLine();
//		String[] buffer = temp.split(Constantself.SPLIT_FLAGD);
//		this.vertexValue = Character.valueOf(buffer[0].charAt(0));
//		this.edgeValue = Character.valueOf(buffer[1].charAt(0));
//	}
	public void readFields(DataInput in) throws IOException {
		this.vertexID = in.readInt();
		this.vertexValue = in.readChar();
		this.edgeValue = in.readChar();
	}

	@Override
//	public void write(DataOutput out) throws IOException {
//		out.writeInt(this.vertexID);
//		String temp=this.vertexValue+Constantself.SPLIT_FLAGD +this.edgeValue;
//		out.writeBytes(temp);
//	}
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.vertexID);
		out.writeChar(this.vertexValue);
		out.writeChar(this.edgeValue);
	}

	@Override
	public boolean equals(Object object) {
		SMEdge edge = (SMEdge) object;
		if ((this.vertexID == edge.getVertexID())&&(this.vertexValue==edge.vertexValue)) {
			return true;
		} else {
			return false;
		}
	}
	@Override
	public void setEdgeValue(String arg0) {
		String[] buffer = arg0.split(Constantself.SPLIT_FLAGD);
		this.vertexValue = Character.valueOf(buffer[0].charAt(0));
		this.edgeValue = Character.valueOf(buffer[1].charAt(0));
	}

}