package com.chinamobile.bcbsp.examples.subgraph;
/**
 * KCentersAggregator.java
 */

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

import com.chinamobile.bcbsp.api.Aggregator;


/**
 * KCentersAggregator
 * 
 * @author moon
 */
public class SMFirstAggregator extends Aggregator<SMFirstAggregateValue> {
//	public static final Log LOG = LogFactory.getLog(SMFirstAggregator.class);
	
		private Integer superStepCount;
		private Integer min_support;
		private int flag=0;
	
		@SuppressWarnings("unchecked")
		@Override
		public SMFirstAggregateValue aggregate(
				Iterable<SMFirstAggregateValue> values) {
	
			SMFirstAggregateValue edgesInfo = new SMFirstAggregateValue();	
			// Init the contents with the first aggregate value.
			Iterator<SMFirstAggregateValue> it = values.iterator();
			
					while (it.hasNext()) {
						SMFirstAggregateValue val = it.next();
						// flag = flag * val.flag;
					//	LOG.info("val:	hashcode  =  "+ val.hashCode());
                        int infosFromOne;
						infosFromOne = val.getValue();
					//	LOG.info("Aggregevalue:	flag = "+ infosFromOne);
	                    flag=flag|infosFromOne;
						
					}
				
					edgesInfo.flag = flag;				
			       return edgesInfo;
		}
	
}