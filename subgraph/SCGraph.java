package com.chinamobile.bcbsp.examples.subgraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.fs.FSDataOutputStream;


public class SCGraph {
	/*
	 * songjianze added 
	 */
	private static SCGraph scgraph;
	public static SCGraph getinstance(TreeSet<String> param){
		if(scgraph==null){
			return new SCGraph(param);
		}
		scgraph.clear();
		scgraph.initialize(param);
		return scgraph;
	}
	
	public void initialize(TreeSet<String> param){
		Iterator<String> it = param.iterator();
		while (it.hasNext()) {
			String edge = it.next();
			String[] array = edge.split(Constantself.SPLIT_FLAGD);
			Integer V1ID = Integer.valueOf(array[0]);
			char V1Value = CommonUtils.StrToChar(array[1]);
			Integer V2ID = Integer.valueOf(array[2]);
			char V2Value = CommonUtils.StrToChar(array[3]);
			char edgeValue = CommonUtils.StrToChar(array[4]);

			SCVertex V1 = null;
			if (getVertexIndexByID(V1ID) >= 0) {// V1存在
				V1 = vertexsList.get(getVertexIndexByID(V1ID));
			} else {
				SCVertex V_new = new SCVertex(V1ID, V1Value);
				vertexsList.add(V_new);
				V1 = V_new;
			}
			SCEdge E1_new = new SCEdge(V2ID, edgeValue);
			V1.addEdge(E1_new);

			SCVertex V2 = null;
			if (getVertexIndexByID(V2ID) >= 0) {// V2存在
				V2 = vertexsList.get(getVertexIndexByID(V2ID));
			} else {
				SCVertex V_new = new SCVertex(V2ID, V2Value);
				vertexsList.add(V_new);
				V2 = V_new;
			}
			SCEdge E2_new = new SCEdge(V1ID, edgeValue);
			V2.addEdge(E2_new);
		}

	
	}//initialize
	
	public static SCGraph getinstance(String standardcode){
		if(scgraph==null){
			return new SCGraph(standardcode);
		}
		scgraph.clear();
		scgraph.initialize(standardcode);
		return scgraph;
	}
	
	public void initialize(String standardcode){

		char[] str = standardcode.toCharArray();
		int VertexNum = (int) Math.sqrt(standardcode.length() * 2);

		int index = 0;
		for (int i = 0; i < VertexNum; i++) {
			SCVertex V_new = new SCVertex(i, str[index]);
			vertexsList.add(V_new);
			index += VertexNum - i;// 对角线位置上，是各个顶点的标签
		}

		index = 1;
		for (int i = 0; i < VertexNum; i++) {
			SCVertex Vi = vertexsList.get(i);
			for (int j = i + 1; j < VertexNum; j++) {
				if (index < standardcode.length() && str[index] != '0') {
					SCEdge E1_new = new SCEdge(j, str[index]);
					Vi.addEdge(E1_new);
					SCVertex Vj = vertexsList.get(j);
					SCEdge E2_new = new SCEdge(i, str[index]);
					Vj.addEdge(E2_new);
				}
				index++;
			}
			index++;
		}
	
	}//initialize(String)
	/*
	 * songjianze added
	 */
	public List<SCVertex> vertexsList = new ArrayList<SCVertex>();

	TreeSet<Integer> flagSet;// 不同分区的最后一个顶点的位置索引

	boolean flag = false;// flagset是否被修改了的标识

	String standCode = new String();

	// 生成编码时用的索引链表
	List<Integer> indexlist = new ArrayList<Integer>();

	/**
	 * 比较器，生成规范编码的前两种优化方法
	 * 
	 * @author moon
	 */
	Comparator<SCVertex> comparator = new Comparator<SCVertex>() {
		public int compare(SCVertex V1, SCVertex V2) {
			// 先排顶点的度（由大到小）
			if (V1.degree != V2.degree) {
				return V2.degree - V1.degree;
			} else {
				// 度相同则按照标签排序（由大到小）
				if (V1.vertexValue != V2.vertexValue) {
					return V2.vertexValue - V1.vertexValue;
				} else {
					// 度和标签都相同，按照编号排序（由小到大）
					return V1.vertexID - V2.vertexID;
				}
			}
		}
	};
	/**
	 * 比较器，进行规范编码第三阶段优化时，每个顶点都对应一个orderlist，邻接列表对，如果两个顶点的分区相同，那么顺序按照顶点对的顺序决定
	 * 
	 * @author moon
	 */
	Comparator<SCVertex> comparator2 = new Comparator<SCVertex>() {
		public int compare(SCVertex V1, SCVertex V2) {
			if (V1.partition == V2.partition) {
				return CommonUtils.compare(V1.orderList, V2.orderList);
			} else {
				return 0;
			}
		}
	};

	/**
	 * 根据set生成图
	 * 
	 * @param 构成这个图的所有边，边用string表示，是一个五元组，id1,label1,id2,label2,edgelabel(id1
	 *            <id2)
	 * @author moon
	 */
	public SCGraph(TreeSet<String> param) {
		Iterator<String> it = param.iterator();
		while (it.hasNext()) {
			String edge = it.next();
			String[] array = edge.split(Constantself.SPLIT_FLAGD);
			Integer V1ID = Integer.valueOf(array[0]);
			char V1Value = CommonUtils.StrToChar(array[1]);
			Integer V2ID = Integer.valueOf(array[2]);
			char V2Value = CommonUtils.StrToChar(array[3]);
			char edgeValue = CommonUtils.StrToChar(array[4]);

			SCVertex V1 = null;
			if (getVertexIndexByID(V1ID) >= 0) {// V1存在
				V1 = vertexsList.get(getVertexIndexByID(V1ID));
			} else {
				SCVertex V_new = new SCVertex(V1ID, V1Value);
				vertexsList.add(V_new);
				V1 = V_new;
			}
			SCEdge E1_new = new SCEdge(V2ID, edgeValue);
			V1.addEdge(E1_new);

			SCVertex V2 = null;
			if (getVertexIndexByID(V2ID) >= 0) {// V2存在
				V2 = vertexsList.get(getVertexIndexByID(V2ID));
			} else {
				SCVertex V_new = new SCVertex(V2ID, V2Value);
				vertexsList.add(V_new);
				V2 = V_new;
			}
			SCEdge E2_new = new SCEdge(V1ID, edgeValue);
			V2.addEdge(E2_new);
		}

	}

	// 显示图结构
	public void show() {
		// 显示顶点信息
		for (int i = 0; i < vertexsList.size(); i++) {
			SCVertex vi = vertexsList.get(i);
			System.out.println("V\t" + vi.vertexID + "\t" + vi.vertexValue);
		}

		// 显示边信息
		for (int i = 0; i < vertexsList.size(); i++) {
			SCVertex Vi = vertexsList.get(i);
			List<SCEdge> edgelist = Vi.edgesList;
			for (int j = 0; j < edgelist.size(); j++) {
				SCEdge e = edgelist.get(j);
				if (!e.flag) {
					SCVertex Vj = getVertexByID(e.vertexID);
					Vj.markEdgeByVID(Vi.vertexID);
					// 显示信息
					System.out.println("E\t" + Vi.vertexID + "\t" + Vj.vertexID
							+ "\t" + e.edgeValue);
				}
			}
		}
	}

	/**
	 * 新加的，用于将频繁子图写入HDFS中 ,Path hdfsfile在主函数中创建，FSDataOutputStream在主函数中创建,最后没有使用
	 * 
	 */
	public void WriteHDFS(FSDataOutputStream out) throws IOException {

		// FSDataOutputStream dos=hdfs.create(hdfsfile);
		for (int i = 0; i < vertexsList.size(); i++) {
			SCVertex vi = vertexsList.get(i);
			String ivertex = new String("V\t" + vi.vertexID + "\t"
					+ vi.vertexValue);
			byte[] readbuf = ivertex.getBytes();
			out.write(readbuf, 0, readbuf.length);
			// System.out.println("V\t"+vi.vertexID+"\t"+vi.vertexValue);
		}

		for (int i = 0; i < vertexsList.size(); i++) {
			SCVertex Vi = vertexsList.get(i);
			List<SCEdge> edgelist = Vi.edgesList;
			for (int j = 0; j < edgelist.size(); j++) {
				SCEdge e = edgelist.get(j);
				if (!e.flag) {
					SCVertex Vj = getVertexByID(e.vertexID);
					Vj.markEdgeByVID(Vi.vertexID);

					String iedge = new String("E\t" + Vi.vertexID + "\t"
							+ Vj.vertexID + "\t" + e.edgeValue);
					byte[] readbuf = iedge.getBytes();
					out.write(readbuf, 0, readbuf.length);
					// System.out.println("E\t"+Vi.vertexID+"\t"+Vj.vertexID+"\t"+e.edgeValue);
				}
			}
		}
		// 在主函数中别忘了关闭out,
	}

	// 由规范编码生成图
	public SCGraph(String standardcode) {
		char[] str = standardcode.toCharArray();
		int VertexNum = (int) Math.sqrt(standardcode.length() * 2);

		int index = 0;
		for (int i = 0; i < VertexNum; i++) {
			SCVertex V_new = new SCVertex(i, str[index]);
			vertexsList.add(V_new);
			index += VertexNum - i;// 对角线位置上，是各个顶点的标签
		}

		index = 1;
		for (int i = 0; i < VertexNum; i++) {
			SCVertex Vi = vertexsList.get(i);
			for (int j = i + 1; j < VertexNum; j++) {
				if (index < standardcode.length() && str[index] != '0') {
					SCEdge E1_new = new SCEdge(j, str[index]);
					Vi.addEdge(E1_new);
					SCVertex Vj = vertexsList.get(j);
					SCEdge E2_new = new SCEdge(i, str[index]);
					Vj.addEdge(E2_new);
				}
				index++;
			}
			index++;
		}
	}

	// 添加顶点
	public void addVertex(SCVertex arg0) {
		vertexsList.add(arg0);
	}

	// 删除顶点
	public void removeVertex(SCVertex arg0) {
		vertexsList.remove(arg0);
	}

	// 得到顶点数目
	public int getVertexNum() {
		return vertexsList.size();
	}

	// 获取给定顶点的索引
	public int getVertexIndexByID(Integer vertexid1) {
		if (vertexid1 != null) {
			for (int i = 0; i < getVertexNum(); i++) {
				if (vertexid1.equals(vertexsList.get(i).vertexID)) {
					return i;
				}
			}
		}
		return -1;
	}

	// 通过顶点ID得到顶点
	public SCVertex getVertexByID(Integer vertexid1) {
		if (vertexid1 != null) {
			for (int i = 0; i < getVertexNum(); i++) {
				if (vertexid1.equals(vertexsList.get(i).vertexID)) {
					return vertexsList.get(i);
				}
			}
		}
		return null;
	}

	// 排序
	public void setSortable() {
		Collections.sort(vertexsList, comparator);
	}

	/**
	 * 修改顶点分区号，if flagset中还没有进行分区，那么进行初始分区，flagset中存的是每个分区最后一个顶点的位置索引
	 * *********** else 如果已经进行了分区，那么需要给每个顶点标记上分区号
	 * 
	 * @author moon
	 */
	public void setPartionID() {
		if (this.flagSet == null) {
			this.flag = true;
			flagSet = new TreeSet<Integer>();
			Integer p = 0;
			vertexsList.get(0).partition = 0;
			for (int i = 1; i < vertexsList.size(); i++) {
				SCVertex V1 = vertexsList.get(i - 1);
				SCVertex V2 = vertexsList.get(i);
				if (V1.degree != V2.degree || V1.vertexValue != V2.vertexValue) {
					p++;
					int flag = i - 1;
					flagSet.add(flag);
				}
				vertexsList.get(i).partition = p;
			}
			int last = vertexsList.size() - 1;
			flagSet.add(last);
		} else {
			Iterator<Integer> it = flagSet.iterator();
			Integer p = 0;
			Integer i1 = 0;
			Integer i2 = 0;
			while (it.hasNext()) {
				i2 = (Integer) it.next();
				for (int i = i1; i <= i2; i++) {
					vertexsList.get(i).partition = p;
				}
				p++;
				i1 = i2 + 1;
			}
		}
	}

	// 根据orderlist调整顶点顺序
	public void setVertexsListByOL() {
		Collections.sort(vertexsList, comparator2);
	}

	// 根据edgelist生成orderlist
	public void setOrderList() {
		for (int i = 0; i < vertexsList.size(); i++) {
			SCVertex Vi = vertexsList.get(i);
			List<SCEdge> edgesList = Vi.edgesList;
			for (int j = 0; j < edgesList.size(); j++) {
				SCEdge e = Vi.edgesList.get(j);
				int V2ID = e.vertexID;
				SCVertex V2 = getVertexByID(V2ID);
				Order order = new Order(V2.partition, e.edgeValue);
				Vi.addOrder(order);
			}
			Vi.setOrderSortable();
		}
	}

	// 根据orderlist修改flagset
	public void setFlagSet() {
		int sum1 = flagSet.size();
		SCVertex V1 = vertexsList.get(0);
		for (int i = 1; i < vertexsList.size(); i++) {
			SCVertex V2 = vertexsList.get(i);
			if (!CommonUtils.equality(V1.orderList, V2.orderList)) {
				int flag = i - 1;
				flagSet.add(flag);
			}
			V1 = V2;
		}
		int sum2 = flagSet.size();
		if (sum2 == sum1) {
			this.flag = false;
		}
	}

	// 该顶点是否存在
	public boolean existV(Integer vID) {
		for (SCVertex vertex : vertexsList) {
			if (vertex.vertexID == vID) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 根据对应的顺序生成上三角编码，不修改邻接表顺序，生成上三角串，并且记录最大的串
	 * 
	 * @author moon
	 * @param 各种排列之后顶点对应的位置不同，不修改原图的邻接表，而是通过list记录相应位置的顶点在原图的位置索引
	 */
	public void getCodeByLP(List<Integer> list) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < vertexsList.size(); i++) {
			SCVertex Vi = vertexsList.get(list.get(i));
			List<SCEdge> edgelist = Vi.edgesList;
			sb.append(vertexsList.get(i).vertexValue);
			for (int col = i + 1; col < vertexsList.size(); col++) {
				int index = Vi.getEdgeIndex(vertexsList.get(list.get(col)));// 求得相应顶点在edgelist中的位置
				if (index >= 0) {
					sb.append(edgelist.get(index).edgeValue);
				} else {
					sb.append("0");
				}
			}
		}
		/**
		 * 测试 for (int j = 0; j < indexlist.size(); j++) {
		 * System.out.print(indexlist.get(j)); }
		 */
		standCode = sb.toString().compareTo(standCode) > 0 ? sb.toString()
				: standCode;
	}

	/**
	 * 根据划分好的分区，遍历每个分区所有排列组合，对每种顺序求上三角编码，遍历过程中最大的编码就是规范编码
	 * 
	 * @author moon
	 */

	public void getMaxCode() {
		Iterator<Integer> it = flagSet.iterator();
		int[] flagArray = new int[flagSet.size()];
		for (int i = 0; i < flagArray.length; i++) {
			if (it.hasNext()) {
				flagArray[i] = (Integer) it.next();
			}
		}

		indexlist.clear();
		for (int i = 0; i < vertexsList.size(); i++) {
			indexlist.add(i);
		}

		int part = 0;// 记录分区个数
		function(flagArray, part);
	}

	/**
	 * 实现各种排列组合的遍历
	 * 
	 * @param flagArray，复制的flagset
	 * @param part
	 */

	private void function(int[] flagArray, Integer part) {
		if (part >= flagArray.length) {
			getCodeByLP(indexlist);
			return;
		}
		int start = 0;// start记录分区起始位置
		if (part > 0) {
			start = flagArray[part - 1] + 1;
		}
		int end = flagArray[part];// 记录分区末尾位置

		if (start < end) {
			List<String> list = CommonUtils.getPermutation(start, end);// 得到这个分区的所有排列组合
			for (int m = 0; m < list.size(); m++) {
				setIndexListByStr(start, end, list.get(m));
				// 只处理了部分分区的排列。
				if (part < flagArray.length - 1) {
					function(flagArray, part + 1);
				}
				// 得到了一个全部分区的排列，然后求编码
				else {
					getCodeByLP(indexlist);
					return;
				}
			}
		} else {
			function(flagArray, part + 1);
		}
	}

	// 根据排列顺序调整vlist顺序
	@SuppressWarnings( { "rawtypes", "unchecked" })
	private void setIndexListByStr(Integer start, Integer end, String param) {
		if (end > start) {
			// System.out.println(param);
			List<Integer> sublist = new ArrayList(indexlist.subList(start,
					end + 1));
			String subParam = null;
			for (int i = 0; i < param.length(); i++) {
				subParam = param.substring(i, i + 1);
				int index = Integer.valueOf(subParam);
				indexlist. set(start + i, sublist.get(index - start));
			}
		}
	}

	public void clearOrders() {
		for (int i = 0; i < vertexsList.size(); i++) {
			SCVertex Vi = vertexsList.get(i);
			Vi.clearOrders();
		}
	}
	public void clear(){
		vertexsList.clear();
		flagSet.clear();
		flag=false;
		indexlist.clear();
		standCode=null;
		
	}

}