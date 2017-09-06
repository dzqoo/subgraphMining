package com.chinamobile.bcbsp.examples.subgraph;

import java.util.TreeSet;

/**
 * 
 * @author moon
 * 
 */
public class StandardCode {

	public static String getstandardCode(SCGraph g) {
		// 按照顶点的度和标签排序
		g.setSortable();

		// 实现第一次分区划分，初始化flagset
		g.setPartionID();

		// 递归生成最终的flagset
		getflagSet(g, g.flagSet);

		// g.show();

		// 生成规范编码
		g.getMaxCode();

		return g.standCode;
	}

	private static void getflagSet(SCGraph g, TreeSet<Integer> flagSet) {
		// 递归终止条件，如果g.flag不改变，表示上一次划分没有变化，或者是分区数等于顶点数了，这样分区终止
		if (!g.flag || flagSet.size() == g.getVertexNum()) {
			return;
		} else {
			g.clearOrders();

			// edgeList->OrderList
			g.setOrderList();
			// // 根据orderlist调整顶点顺序
			g.setVertexsListByOL();
			// g.show();

			// Order->flagSet
			g.setFlagSet();

			// flagList->Partition
			if (g.flag) {
				g.setPartionID();
			}

			getflagSet(g, g.flagSet);
		}
	}

	/*
	 * public static void main(String[] args) { // TreeSet<String> set = new
	 * TreeSet<String>(); // set.add("0,a,1,a,x"); // set.add("0,a,6,a,x"); //
	 * set.add("0,a,7,a,x"); // set.add("1,a,2,a,x"); // set.add("1,a,5,a,x"); //
	 * set.add("2,a,3,a,x"); // set.add("2,a,4,a,x"); // KMGraph graph = new
	 * KMGraph(set); // StandardCode sc = new StandardCode(); // String code =
	 * sc.getstandardCode(graph); // System.out.println(code);
	 * 
	 * String standcode = new String("axxx0000a00xx00a000xxa0000a000a00a0a");
	 * KMGraph graph2 = new KMGraph(standcode); StandardCode sc = new
	 * StandardCode(); String code = sc.getstandardCode(graph2);
	 * System.out.println(code); }
	 */
}