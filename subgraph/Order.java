package com.chinamobile.bcbsp.examples.subgraph;

/**
 *规范编码优化第三阶段顶点对应邻接列表对 
 * @author moon
 */
public class Order {

	// 区域编号
	private Integer area;

	// 边标签
	private char edgeValue;

	public Order(Integer area, char edgeValue) {
		this.area = area;
		this.edgeValue = edgeValue;
	}

	public Integer getArea() {
		return area;
	}

	public void setArea(Integer area) {
		this.area = area;
	}

	public char getEdgeValue() {
		return edgeValue;
	}

	public void setEdgeValue(char edgeValue) {
		this.edgeValue = edgeValue;
	}

	public boolean equals(Order order) {
		if (this.area != order.getArea()
				|| this.edgeValue != order.getEdgeValue()) {
			return false;
		}
		return true;
	}

	public int compare(Order order) {
		if (this.getArea() != order.getArea()) {
			return this.getArea() - order.getArea();
		} else {
			return order.getEdgeValue() - this.getEdgeValue();
		}
	}

}