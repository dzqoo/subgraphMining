package com.chinamobile.bcbsp.examples.subgraph;

/**
 * KMVertex.java
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Vertex;

/**
 * KMVertex
 * Implementation of Vertex for Sub_Mining
 * 
 * @author moon
 */
public class SMVertex extends Vertex<Integer, Character, SMEdge> {

	public static final Log LOG = LogFactory.getLog(SMVertex.class);
	int vertexID = 0;
    char vertexValue = 0;
    List<SMEdge> edgesList = new ArrayList<SMEdge>();
    
    int edgsNum = 1;
//    HashMap<String, List<String>> codeE = new HashMap<String, List<String>>();
    
	@Override
	public void addEdge(SMEdge edge) {
		this.edgesList.add(edge);
	}

	@Override
	public void fromString(String vertexData) throws Exception {
//		LOG.info("处理的顶点以及顶点对应边序列为："+vertexData);
		String[] buffer = vertexData.split(Constantself.KV_SPLIT_FLAG);
//		LOG.info("处理的顶点："+buffer[0]);
//		LOG.info("处理的顶点对应边："+buffer[1]);
//        String[] vBuffer = buffer[0].split(Constantself.SPLIT_FLAGD);
        String[] vBuffer = buffer[0].split(Constantself.SPLIT_FLAG);
        this.vertexID = Integer.valueOf(vBuffer[0]);
        this.vertexValue = Character.valueOf(vBuffer[1].charAt(0));
        
        if (buffer.length > 1) { // There has edges.
        	if(buffer[1].contains(Constantself.SPLIT_FLAG)){
        		 String[] eBuffer = buffer[1].split(Constantself.SPLIT_FLAG);
                 for (int i = 0; i < eBuffer.length; i ++) {
                     SMEdge edge = new SMEdge();
                     edge.fromString(eBuffer[i]);
                     this.edgesList.add(edge);
                 }
    		}else{
    			SMEdge edge = new SMEdge();
                edge.fromString(buffer[1]);
                this.edgesList.add(edge);
    		}
           
        }
	}

	@Override
	public List<SMEdge> getAllEdges() {
		return this.edgesList;
	}

	@Override
	public int getEdgesNum() {
		return this.edgesList.size();
	}

	@Override
	public Integer getVertexID() {
		return this.vertexID;
	}

	@Override
	public Character getVertexValue() {
		return this.vertexValue;
	}

	@Override
	public String intoString() {
//		String buffer = vertexID + Constantself.SPLIT_FLAGD + vertexValue;
		String buffer = vertexID + Constantself.SPLIT_FLAG+ vertexValue;
        buffer = buffer + Constantself.KV_SPLIT_FLAG;
        int numEdges = edgesList.size();
//        if (numEdges != 0) {
//            buffer = buffer + edgesList.get(0).intoString();
//        }
//        for (int i = 1; i < numEdges; i ++) {
//            buffer  = buffer + Constantself.SPLIT_FLAG + edgesList.get(i).intoString();
//        }
        
        return buffer;
	}
	
	public String intoStrSelf() {
	    return vertexID + Constantself.SPLIT_FLAGD + vertexValue;
    }

	@Override
	public void removeEdge(SMEdge edge) {
		this.edgesList.remove(edge);
	}

	@Override
	public void setVertexID(Integer arg0) {
		this.vertexID = arg0;
	}

	@Override
	public void setVertexValue(Character arg0) {
		this.vertexValue = arg0;
	}

	@Override
	public void updateEdge(SMEdge edge) {
		removeEdge(edge);
        this.edgesList.add(edge);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.vertexID = in.readInt();
        this.vertexValue = in.readChar();
//        this.edgesList.clear();
//        int numEdges = in.readInt();
//        SMEdge edge;
//        for (int i = 0; i < numEdges; i++) {
//            edge = new SMEdge();
//            edge.readFields(in);
//            this.edgesList.add(edge);
//        }
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.vertexID);
        out.writeChar(this.vertexValue);
//        out.writeInt(this.edgesList.size());
//        for (SMEdge edge : edgesList) {
//            edge.write(out);
//        }
	}

//	@Override
//    public int hashCode() {
//        return Integer.valueOf(this.vertexID).hashCode();
//    }
	
//	public void addCodeEMap(String key, String edgeInfo){
//	   // List<String> l = codeE.get(key);
//	    if(!codeE.containsKey(key)){
//	    	 List<String>  l = new ArrayList<String>();
//	    	 l.add(edgeInfo);
//	    	 codeE.put(key, l);
//	    }
//	    else{
//	    	codeE.get(key).add(edgeInfo);
//	    }
//	    
//	}
}