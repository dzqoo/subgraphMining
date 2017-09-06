package com.chinamobile.bcbsp.examples.subgraph;

/**
 * 边表示成五元组<id1,label1,id2,label2,edgelabel>
 * 
 * @author moon
 * 
 */
public class Five_Tuple_Edge implements Comparable<Object> {

	private int ID_X;
	private char label_x;
	private int ID_Y;
	private char label_y;
	private char edge_label;

	public Five_Tuple_Edge(int _x, char lx, int _y, char ly, char el) {
		ID_X = _x;
		label_x = lx;
		ID_Y = _y;
		label_y = ly;
		edge_label = el;
	}

	// public int hashcode()
	// {
	// int result=ID_X+label_x+ID_Y+label_y+edge_label;
	// return result;
	//		  
	// }
	// public boolean equal( Five_Tuple_Edge a )
	// {
	// if((a.ID_X==this.ID_X)&&(a.label_x==this.label_x)&&(a.ID_Y==this.ID_Y)&&(a.label_y==this.label_y)&&(a.edge_label==this.edge_label))
	// return true;
	// else
	// return false;
	//		  
	// }
	//	  
	@Override
	public int compareTo(Object a) {
		Five_Tuple_Edge s = (Five_Tuple_Edge) a;

		if (s.ID_X > this.ID_X) {
			return -1;
		} else if (s.ID_X < this.ID_X) {
			return 1;
		} else {
			if (s.ID_Y > this.ID_Y) {
				return -1;
			} else if (s.ID_Y < this.ID_Y) {
				return 1;
			}
		}
		return 0;

	}

	public String Get_Tuple() {
		String s = new String(ID_X + "," + label_x + "," + ID_Y + "," + label_y
				+ "," + edge_label);
		return s;

	}


}
