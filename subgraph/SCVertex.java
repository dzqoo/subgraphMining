package com.chinamobile.bcbsp.examples.subgraph;
/**
 * KMVertex.java
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;



public class SCVertex {

	// 顶点id
	public Integer vertexID = 0;
	// 顶点标签
	public char vertexValue = 0;
	// 求规范编码优化第三阶段顶点所在分区
	Integer partition = 0;
	// 顶点的度，为了求规范编码第一阶段优化，需要按照顶点的度排序
	Integer degree = 0;
	// 邻接表存储
	public List<SCEdge> edgesList = new ArrayList<SCEdge>();
	// 求规范编码优化第三阶段顶点所对应的邻接列表对。规范对<pair(对应的边的另一个顶点的分区partition,对应的边label)>
	List<Order> orderList = new ArrayList<Order>();
    //order比较器，按照分区号排序
	Comparator<Order> comparator = new Comparator<Order>() {
		public int compare(Order E1, Order E2) {
			return E1.getArea() - E2.getArea();
		}
	};

	public SCVertex() {

	}

	public SCVertex(Integer vertexID, char vertexValue) {
		this.vertexID = vertexID;
		this.vertexValue = vertexValue;

	}
	//显示完一边之后，把对称边标记上
	//不用在显示了
	public void markEdgeByVID(int VID){
		for (int i = 0; i < edgesList.size(); i++) {
			SCEdge edge = edgesList.get(i);
			if(edge.vertexID == VID){
				edge.flag = true;
			}
		}
	}


	// 增加一条边
	public void addEdge(SCEdge edge) {
		this.edgesList.add(edge);
		degree++;
	}

	// 增加一个规范对
	public void addOrder(Order order) {
		this.orderList.add(order);
	}

	// 移除一条边
	public void removeEdge(SCEdge edge) {
		this.edgesList.remove(edge);
		degree--;
	}

	// 清空规划对
	public void clearOrders() {
		this.orderList.clear();
	}

	//修改一条边
	public void updateEdge(SCEdge edge) {
		removeEdge(edge);
		this.edgesList.add(edge);
	}

	public int hashCode() {
		return Integer.valueOf(this.vertexID).hashCode();
	}

	//使得规范对有序
	public void setOrderSortable() {
		Collections.sort(orderList, comparator);
	}

	//获取顶点在当前顶点边集合中的位置索引
	public int getEdgeIndex(SCVertex vertex) {
		int result = -1;
		for (int i = 0; i < edgesList.size(); i++) {
			if (edgesList.get(i).vertexID == vertex.vertexID) {
				result = i;
				break;
			}
		}
		return result;
	}
	//获取顶点与当前顶点对应边的边标签
	public char getEdgeValue(SCVertex vertex){
		char result = '0';
		for (int i = 0; i < edgesList.size(); i++) {
			if (edgesList.get(i).vertexID == vertex.vertexID) {
				result = edgesList.get(i).edgeValue;
				break;
			}
		}
		return result;
	}
}