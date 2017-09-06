package com.chinamobile.bcbsp.examples.subgraph;
/**
 * KMEdge.java
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class SCEdge {

	public Integer vertexID = 0;
	public char edgeValue = 0;
	
	//处理标识
	public boolean flag = false;

	public SCEdge() {

	}

	public SCEdge(Integer vertexID, char edgeValue) {
		this.vertexID = vertexID;
		this.edgeValue = edgeValue;
	}

	public void fromString(String edgeData) {
		String[] buffer = edgeData.split(Constantself.SPLIT_FLAG);
		this.vertexID = Integer.valueOf(buffer[0]);
		this.edgeValue = buffer[1].toCharArray()[0];
	}

	public char getEdgeValue() {
		return this.edgeValue;
	}

	public Integer getVertexID() {
		return this.vertexID;
	}

	public String intoString() {
		return this.vertexID + Constantself.SPLIT_FLAG + this.edgeValue;
	}

	public void setEdgeValue(char arg0) {
		this.edgeValue = arg0;
	}

	public void setVertexID(Integer arg0) {
		this.vertexID = arg0;
	}

	public void readFields(DataInput in) throws IOException {
		this.vertexID = in.readInt();
		this.edgeValue = in.readChar();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.vertexID);
		out.writeChar(edgeValue);
	}

	public boolean equals(Object object) {
		SCEdge edge = (SCEdge) object;
		if (this.vertexID == edge.getVertexID()) {
			return true;
		} else {
			return false;
		}
	}
}