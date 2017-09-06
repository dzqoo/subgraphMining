package com.chinamobile.bcbsp.examples.subgraph;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.AggregationContextInterface;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.util.BSPJob;

//public class SMFirstAggregateValue extends
//		AggregateValue<HashMap<String, List<String>>> {
public class SMFirstAggregateValue extends
	AggregateValue<Integer,Sub_Message> {
	public static final Log LOG = LogFactory.getLog(SMFirstAggregateValue.class);
	private HashMap<String, List<String>> result1 = new HashMap<String, List<String>>();


	Integer superStepCount;

	Integer min_support;

	Integer flag = 0;
	@Override
	public Integer getValue() {
		return flag;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void initValue(String arg0) {
//	LOG.info("聚集值中的initvalue被调用 " );
//		LOG.info("参数arg0: "+arg0 );
		flag=Integer.valueOf(arg0);
		
	}

	@Override
	public String toString() {
	//	LOG.info("聚集值中Tostring被调用 " );
	//	LOG.info("聚集值中flag = "+flag );
		return String.valueOf(flag);

	}

	@Override
	public void initValue(Iterator<Sub_Message> messages,
			AggregationContextInterface context) {
	//	LOG.info("sjz test in initValue function");
		// TODO Auto-generated method stub
	// 根据节点数据构造自己想要的数据
		superStepCount = context.getCurrentSuperStepCounter();
		BSPJob bsp = context.getJobConf();
		flag=0;

		 if ((superStepCount!=1)&&(superStepCount % 2 == 1)) {
			 flag=0;
			// LOG.info("Aggregevalue:	flag = "+ flag);
			 result1.clear();//一个任务多个顶点公共使用，所以使用之前先清空。
			String messageValue = null;
			while (messages.hasNext()) {
				messageValue = ((Sub_Message)(messages.next())).getContent();
				//LOG.info("messageValue is :"+messageValue);
				String msg[] = messageValue.split(Constantself.KV_SPLIT_FLAG);
				addToResult1(msg[0], msg[1]);
			}
			//LOG.info("==============================================================================");
			Set<String> keySet = result1.keySet();
			for (Iterator it1 = keySet.iterator(); it1.hasNext();) {
				String key = (String) it1.next();

				List<String> values = result1.get(key);

				String outkey = null;
				String outvalue = null;
				ArrayList<Set<Pair>> edgeset = new ArrayList<Set<Pair>>();
				Set<String> normalcode = new TreeSet<String>();
				for (String val : values) {
					String num[] = val.toString().split(
							Constantself.SEPA_SPLIT_FLAG);
					String splitnum0[] = num[0].split(Constantself.SPLIT_FLAG);
					String splitnum1[] = num[1].split(Constantself.SPLIT_FLAG);
					normalcode.add(splitnum0[1]);
					normalcode.add(splitnum1[1]);
					Set<Pair> embedset = new HashSet<Pair>();
					if (num[0].contains(Constantself.AND_SPLIT_FLAG)) {
						String getIpair0[] = splitnum0[0]
								.split(Constantself.AND_SPLIT_FLAG);
						String getIpair1[] = splitnum1[0]
								.split(Constantself.AND_SPLIT_FLAG);
						for (int i = 0; i < getIpair0.length; i++) {
							String Ipair0[]=getIpair0[i].split(Constantself.SPLIT_FLAGD);
							 Pair p0 = new Pair(Integer.parseInt(Ipair0[0]), Integer.parseInt(Ipair0[2]));// 抽取出顶点id,
							embedset.add(p0);
								String Ipair1[]=getIpair1[i].split(Constantself.SPLIT_FLAGD);
								 Pair p1 = new Pair(Integer.parseInt(Ipair1[0]), Integer.parseInt(Ipair1[2]));// 抽取出顶点id,
									embedset.add(p1);
						}
					} else {
						String Ipair0[]=splitnum0[0].split(Constantself.SPLIT_FLAGD);
						 Pair p0 = new Pair(Integer.parseInt(Ipair0[0]), Integer.parseInt(Ipair0[2]));// 抽取出顶点id,
						embedset.add(p0);
						 String Ipair1[]=splitnum1[0].split(Constantself.SPLIT_FLAGD);
						 Pair p1 = new Pair(Integer.parseInt(Ipair1[0]), Integer.parseInt(Ipair1[2]));// 抽取出顶点id,
						embedset.add(p1);
					}
					if(!edgeset.contains(embedset))// 一个频繁子图可能由多对图连接得到，为了去重
					edgeset.add(embedset);
				}
				if(edgeset.size()>=min_support)
				{
				LOG.info("before CountMis in initValue");
				LOG.info("test edgeset:");
				for(Iterator it = edgeset.iterator();it.hasNext();){
					Set<Pair> s1 = (Set<Pair>) it.next();
					for(Iterator<Pair> it2 = s1.iterator();it2.hasNext();){
						Pair p= it2.next();
						LOG.info("first:"+p.getfirst()+"second:"+p.getsecond());	
					}
					LOG.info("-----------------------------------------------------------");
				}
				LOG.info("end of test edgeset!");
				LOG.info("edgeset size : "+edgeset.size());
//				changed by songjianze
				Graph_Click click = new Graph_Click();
				boolean count = click.new InstanceGraph(edgeset,min_support).CountMIS();
				
//					Graph_Click click = Graph_Click.getInstance();	
//				boolean count = click.getInstanceGraph(edgeset, min_support).CountMIS();
//				LOG.info("after CountMis in initValue");
				if (count) {
					flag = 1;
				}
				}
			}
			
		//	LOG.info("Aggregevalue change:	flag = "+ flag);
		}
	}
	public void print(HashMap<String,List<String>> arg)
	{
		LOG.info("打印:"+arg +"中的内容：");
		Set<String> keySet = arg.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) {

			String key = (String) it.next();
			LOG.info("  key:"+key);

			List<String> l = arg.get(key);
			int ls = l.size();

			for (int i = 0; i < ls; i++) {
				LOG.info("  key:"+key+" 对应的值 :"+l.get(i));
			}
		}
		LOG.info("打印:"+arg +"中的内容完毕");
	}
	private void addToResult1(String key, String edgeInfo) {
		//List<String> l = result1.get(key);
		if ( !result1.containsKey(key)) {
			List<String> l = new ArrayList<String>();
		     l.add(edgeInfo);
		     result1.put(key, l);
		}
		else{
			result1.get(key).add(edgeInfo);
		}

	}

	@Override
	public void setValue(Integer arg0) {
		this.flag = flag;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		 flag = in.readInt();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(flag);
	}
	@Override
	public void initBeforeSuperStep(SuperStepContextInterface context) {
		superStepCount = context.getCurrentSuperStepCounter();
		min_support = Integer.valueOf(context.getJobConf().get(
				SubGraphBSP.MIN_SUPPORT));
		result1.clear();
	}
	
		
	
}