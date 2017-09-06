package com.chinamobile.bcbsp.examples.subgraph;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;



/**
 * 
 * @author moon
 *
 */

public class CommonUtils {


	public static char StrToChar(String str) {
		char[] chars = str.toCharArray();
		if (chars.length > 0) {
			return chars[0];
		}
		return '$';
	}

	public static List<String> getPermutation(Integer start, Integer end) {
		int[] bits = new int[end - start + 1];
		int j = 0;
		for (int i = start; i <= end; i++) {
			bits[j] = i;
			j++;
		}
		List<String> list = new ArrayList<String>();
		sort("", bits,list);
		return list;
	}

	private static List<String> sort(String prefix, int[] a, List<String> list) {
		if (a.length == 1) {
			list.add(prefix + a[0]);
		}
		for (int i = 0; i < a.length; i++) {
			sort(prefix + a[i], copy(a, i), list);
		}
		return list;
	}

	private static int[] copy(int[] a, int index) {
		int[] b = new int[a.length - 1];
		System.arraycopy(a, 0, b, 0, index);
		System.arraycopy(a, index + 1, b, index, a.length - index - 1);
		return b;
	}

	public static void swap(List<Object> list, int a, int b) {
		Object objA = list.get(a);
		list.set(a, list.get(b));
		list.set(b, objA);
	}

	public static boolean equality(List<Order> orderList1,
			List<Order> orderList2) {
		if (orderList1.size() != orderList2.size()) {
			return false;
		}
		for (int i = 0; i < orderList1.size(); i++) {
			if (!orderList1.get(i).equals(orderList2.get(i))) {
				return false;
			}
		}
		return true;
	}

	public static int compare(List<Order> orderList1, List<Order> orderList2) {
		for (int i = 0; i < orderList1.size(); i++) {
			Order o1 = orderList1.get(i);
			Order o2 = orderList2.get(i);
			int r = o1.compare(o2);
			if (r != 0) {
				return r;
			}
		}
		return 0;
	}

	public static byte[] getBytes(char[] chars) {
		Charset cs = Charset.forName("UTF-8");
		CharBuffer cb = CharBuffer.allocate(chars.length);
		cb.put(chars);
		cb.flip();
		ByteBuffer bb = cs.encode(cb);

		return bb.array();
	}

	public static String union(String src1,String src2)
    {
        String des = new String();
        String splitsrc1[] = src1.split("&");
        String splitsrc2[] = src2.split("&");
        TreeSet<Five_Tuple_Edge> edge = new TreeSet<Five_Tuple_Edge>();
        for (int i = 0; i < splitsrc1.length; i++) {
            String get_tuple1[] = splitsrc1[i].split(",");
            Five_Tuple_Edge a = new Five_Tuple_Edge(
                    Integer.parseInt(get_tuple1[0]),
                    get_tuple1[1].charAt(0),
                    Integer.parseInt(get_tuple1[2]),
                    get_tuple1[3].charAt(0), get_tuple1[4].charAt(0));

            edge.add(a);
            String get_tuple2[] = splitsrc2[i].split(",");
            Five_Tuple_Edge e = new Five_Tuple_Edge(
                    Integer.parseInt(get_tuple2[0]),
                    get_tuple2[1].charAt(0),
                    Integer.parseInt(get_tuple2[2]),
                    get_tuple2[3].charAt(0), get_tuple2[4].charAt(0));

            edge.add(e);
        }
        StringBuffer edgeunion = new StringBuffer();
        Iterator<Five_Tuple_Edge> it = edge.iterator();
        while (it.hasNext()) {
            Five_Tuple_Edge a = it.next();
            String s = a.Get_Tuple();
            if (it.hasNext()) {
                edgeunion.append(s + "&");
            } else
                edgeunion.append(s);
        }
        des = edgeunion.toString();
   //     System.out.println("des:" + des);
        return des;
    }
/**
 * key的可能形式：单个顶点 123 一对顶点123,234   多对顶点  123，234:234,456
 * */
	public static Integer getSmallVID(String key){
		 int small = Integer.MAX_VALUE;
		if(key.contains(Constantself.SPLIT_FLAG))
		{
		 String[] idPairs = key.split(Constantself.SPLIT_FLAG);
		 for(int i = 0; i<idPairs.length; i++){
			 String[] ids = idPairs[i].split(Constantself.SPLIT_FLAGD);
			 for(int j = 0; j<ids.length; j++){
				 int id = Integer.valueOf(ids[j]);
				 if(id < small){
					 small = id;
				 }
			 }
		 }
		}
		else if(key.contains(Constantself.SPLIT_FLAGD))
		{
			String[] ids =key.split(Constantself.SPLIT_FLAGD);
			 for(int j = 0; j<ids.length; j++){
				 int id = Integer.valueOf(ids[j]);
				 if(id < small){
					 small = id;
				 }
			 }
		}
		else
			small=Integer.valueOf(key);
		 return small;
	}

}