package com.chinamobile.bcbsp.examples.subgraph;


//import SMFirstAggregateValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * @version 2014-3-17
 * @author moon
 * 只需要知道当前能形成的团的个数>=min_support即可。所以不需要完整求出最大团。
 * 另外传入的参数用treeset存储，所以在createGraph时候，建立连接边时可以少一重循环
 */
public class Graph_Click {
	/*
	 * added by songjianze 
	 */
	private static Graph_Click graphclick;
	private InstanceGraph insGraph;
	
	public static final Log LOG = LogFactory.getLog(Graph_Click.class);
	
	public static Graph_Click getInstance(){
		if(graphclick==null)
			return new Graph_Click();
		return graphclick;
	}
	
	public InstanceGraph getInstanceGraph(ArrayList<Set<Pair>> aem,int amin_support){
		if(insGraph==null){
			return new InstanceGraph( aem, amin_support);
		}else{
			insGraph.clear();
			insGraph.em=aem;
			insGraph.min_support=amin_support;
			return insGraph;
		}
	}
	
	/*
	 * added by songjianze 
	 */
	public class ArcNode {

		public int vertex;

		public ArcNode next;
	}

	public class InstanceGraph {

		public ArrayList<ArcNode> AdjList;
		ArrayList<Set<Pair>> em;
        
		boolean result=false;
		int min_support;
		int edgecount=0;
		int n; // 图的顶点数
		int cn; // 当前顶点数
		int bestn; // 当前最大顶点数
		int[] x; // 当前解
		//int[] bestx; // 当前最优解

		public InstanceGraph(ArrayList<Set<Pair>> em) {
			this.em = em;
		}
		public InstanceGraph(ArrayList<Set<Pair>> em,int min_support) {
			this.em = em;
			this.min_support=min_support;
		}
		
		/**
		 * 计算出来最大团为min_support时候的边数
		 * @param min_support
		 * @version 2014-3-18
		 * @return
		 */
		// 构造实例图的补图
		public boolean CreateInstanceGraph() {
			AdjList = new ArrayList<ArcNode>();

			int flag;
			int i, j;
			ArcNode pre;
			int board=min_support*(min_support-1)/2;
			// 每个实例图抽象为一个顶点
			for (i = 0; i < em.size(); i++) {
				ArcNode p = new ArcNode();
				p.vertex = i;
				p.next = null;
				AdjList.add(p);
			}
			// 如果两个实例图有公共边，那么这两个实例图对应的顶点在补图中没有边相连
			for (i = 0; i < em.size() - 1; i++) {
				pre = AdjList.get(i);
				for (j = i + 1; j < em.size(); j++) {
					flag = 1;
					Set<Pair> s1 = em.get(i);
					Set<Pair> s2 = em.get(j);
					Pair p1, p2;
					Iterator<Pair> it1 = s1.iterator();
					while (it1.hasNext())
					{
						p1 = (Pair) it1.next();
						Iterator<Pair> it2 = s2.iterator();
						while (it2.hasNext())
						{
							p2 = (Pair) it2.next();
							if (p2.equal(p1)) {
								flag = 0;
								break;
							}	
						}
						if(flag==0)
							break;
						
					}
					if (flag == 1) {
						ArcNode q = new ArcNode();
						q.vertex = j;// 补图有边
						q.next = pre.next;
						pre.next = q;
						edgecount++;
						if((edgecount==1)&&(min_support==2)){
							return true;
						}
							
					}
				}
			}
			/**
			 * 所需要的最少边数剪枝
			 */
			if(edgecount>=board)
				return true;
			else
				return false;
		}

		public boolean Connect(int i, int j) {
			ArcNode p = (ArcNode) AdjList.get(i);
			while (p != null) {
				if (p.vertex != j) {
					p = p.next;
				} else {
					return true;
				}
			}
			return false;
		}

		// 计算最大团
		public void Backtrack(int i) {
			// 到达叶子节点
			if (i > n-1) {
				bestn = cn; // 只需要这个count=cn;
				if(cn>=min_support)
				{
                    result=true;				
				}
				return;
			}
			// 检查顶点i与当前团的连接
			int OK = 1;
			for (int j = 0; j < i; j++) {
				if (x[j] == 1 && (!Connect(j, i))) {
					// i与j 不连接
					OK = 0;
					break;
				}
			}

			// 进入座子树
			if (OK == 1) {
				x[i] = 1;
				cn++;
				if(cn>=min_support)
				{
					result=true;
					return;
				}
				Backtrack(i + 1);
				if(result)
					return;
				x[i] = 0;
				cn--;
			}
			// 进入右子树
			if (cn + n - i-1 >= bestn) {
				x[i] = 0;
				Backtrack(i + 1);
			}
		}

		// 计算最大团，返回最大团的顶点个数
		public boolean CountMIS() {
			cn = 0;
			bestn = 0;

			n = em.size();
	
			 if(CreateInstanceGraph())
			 {
				 if(((edgecount==1)&&(min_support==2))||(edgecount==(em.size()*(em.size()-1)/2))){
					 return true;
				 }
						
			   x = new int[n];
			   Backtrack(0);
			   return result;
			 }
			 else {
				 return false;
			 }
				 
		}
		
		/*
		 * added by songjianze 
		 * just for test
		 */
		public void clear(){
			
			if(AdjList!=null)
				AdjList.clear();
			if(em!=null)
				em.clear();
			result=false;
			min_support=0;
			edgecount=0;
			n=0;
			cn=0;
			bestn=0;
			x=null;
		}
		
		/*
		 * added by songjianze 
		 */
		
		public ArrayList<ArcNode> computeAdijList(){
			CreateInstanceGraph();
			return AdjList;
		}
		/*
		 * public static void main(String args[]){ Pair p1 = new Pair();
		 * p1.first = 1; p1.second = 2; Pair p2 = new Pair(); p2.first = 2;
		 * p2.second = 3; Pair p3 = new Pair(); p3.first = 3; p3.second = 1;
		 * Pair p4 = new Pair(); p4.first = 2; p4.second = 3; Pair p5 = new
		 * Pair(); p5.first = 2; p5.second = 4; Pair p6 = new Pair(); p6.first =
		 * 3; p6.second = 4; Set<Pair> set1 = new TreeSet<Pair>(); Set<Pair>
		 * set2 = new TreeSet<Pair>(); set1.add(p1); set1.add(p2);
		 * set1.add(p3); set2.add(p4); set2.add(p5); set2.add(p6); ArrayList<Set<Pair>>
		 * em = new ArrayList<Set<Pair>>(); em.add(set1); em.add(set2);
		 * InstanceGraph ig = new InstanceGraph(em); int bestn = ig.CountMIS();
		 * System.out.println(bestn); }
		 */
	}
}
