package com.chinamobile.bcbsp.examples.subgraph;

import java.util.HashSet;
import java.util.Set;







/**
 * @version 2014-3-17
 * @author moon 用treeset存pair, 需要pair实现comparable
 */
public class Pair implements Comparable {
	private int first;
	private int second;

	public Pair(int first, int second) {
		this.first = first;
		this.second = second;
	}

	public boolean equal(Pair a) {
		if (((a.first == this.first) && (a.second == this.second))
				|| ((a.first == this.second) && (a.second == this.first)))
			return true;
		else
			return false;

	}
	
	//sjz added equals method
	public boolean equals(Object obj) {
		//System.out.println("equals");
		if(obj instanceof Pair){
			Pair a = (Pair) obj;
			if (((a.first == this.first) && (a.second == this.second))
					|| ((a.first == this.second) && (a.second == this.first)))
				return true;
			else
				return false;
		}
			return super.equals(obj);
		

	}
	

	public int hashCode(){
		//System.out.println("hashCode");
		return first+second;
	}
	// public int compareTo(Pair p1) {
	// if (((p1.first == this.first) && (p1.second == this.second))
	// || ((p1.first == this.second) && (p1.second == this.first))) {
	// return 0;
	// }
	// return 1;
	// }
	public int getfirst() {
		return first;
	}

	public int getsecond() {
		return second;
	}

	public void setfirst(int first) {
		this.first = first;
	}

	public void setsecond(int second) {
		this.second = second;
	}

	@Override
	public int compareTo(Object o) {

		Pair p1 = (Pair) o;
		if (((p1.first == this.first) && (p1.second == this.second))
				|| ((p1.first == this.second) && (p1.second == this.first))) {
			return 0;
		} else if (p1.first == this.first) {
			if (this.second > p1.second)
				return 1;
			else
				return -1;
		} else if (this.first <p1.first) {
			return -1;
		} else
			return 1;
	}
	
	/*public static void main(String args[]){
		Set<Pair> s=new HashSet<Pair>();
		Pair p1=new Pair(1,4);
		Pair p2= new Pair(2,4);
		Pair p3 = new Pair(1,4);
		Pair p4 = new Pair(2,3);
		s.add(p1);
		s.add(p2);
		s.add(p3);
		s.add(p4);
		System.out.println("s size: "+s.size());
	}*/
	
}