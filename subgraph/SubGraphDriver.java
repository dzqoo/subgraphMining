package com.chinamobile.bcbsp.examples.subgraph;


import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.io.KeyValueBSPFileInputFormat;
import com.chinamobile.bcbsp.io.TextBSPFileOutputFormat;
import com.chinamobile.bcbsp.util.BSPJob;

public class SubGraphDriver {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 6) {
			System.out.println("Usage: <nSupersteps>  <FileInputPath>  <FileOutputPath>  <MIN_SUPPORT> <VERTEX_NUM> <PartitionNum>" +
					"  <SplitSize(MB)>  <SendThreshold>  <SendCombineThreshold> " +
					"  <MemDataPercent>  <Beta>  <HashBucketNum>  <MsgPackSize>");
			System.exit(-1);
			
		}
		
		// Set the base configuration for the job
		BSPConfiguration conf = new BSPConfiguration();
		BSPJob bsp = new BSPJob(conf, SubGraphDriver.class);
		bsp.setJobName("SubGraphMining");
		//bsp.setNumPartition(2);
		bsp.setNumSuperStep(Integer.parseInt(args[0]));
		bsp.setPartitionType(Constants.PARTITION_TYPE.HASH);
		bsp.setPriority(Constants.PRIORITY.NORMAL);
		//bsp.setWritePartition(com.chinamobile.bcbsp.partition.NotDivideWritePartition.class);
		//bsp.setWritePartition(com.chinamobile.bcbsp.partition.HashWritePartition.class);
		// Set the BSP.class
		bsp.setBspClass(SubGraphBSP.class);
		bsp.setVertexClass(SMVertex.class);
		bsp.setEdgeClass(SMEdge.class);
		bsp.setMessageClass(Sub_Message.class);
		
		// Set the InputFormat.class and OutputFormat.classl
		bsp.setInputFormatClass(KeyValueBSPFileInputFormat.class);
		bsp.setOutputFormatClass(TextBSPFileOutputFormat.class);
		
		// Set the InputPath and OutputPath
		KeyValueBSPFileInputFormat.addInputPath(bsp, new Path(args[1]));
		TextBSPFileOutputFormat.setOutputPath(bsp, new Path(args[2]));
		
		// Set the graph data implementation version as disk version.
//		bsp.setGraphDataVersion(bsp.DISK_VERSION);
//		bsp.setGraphDataVersion(bsp.MEMORY_VERSION);
//		 Set the message queues implementation version as disk version.
//		bsp.setMessageQueuesVersion(bsp.DISK_VERSION);
//		bsp.setMessageQueuesVersion(bsp.MEMORY_VERSION);
		bsp.setReceiveCombinerSetFlag(false);
		bsp.setMaxProducerNum(38);
		bsp.setCommunicationOption(Constants.RPC_BYTEARRAY_VERSION);
		// Set the graph data implementation version as disk version.
		bsp.setGraphDataVersion(bsp.BYTEARRAY_VERSION);
		/**
		 * @version 2014-3-27 no use aggregator
		 */
		// Register the aggregator.
		bsp.registerAggregator(SubGraphBSP.SUBGRAPH_INFO, SMFirstAggregator.class, SMFirstAggregateValue.class);
		bsp.completeAggregatorRegister();
		// Set the MIN_SUPPORT
		bsp.set(SubGraphBSP.MIN_SUPPORT, args[3]);
		bsp.set(SubGraphBSP.VERTEX_NUM, args[4]);
		bsp.setNumPartition(Integer.parseInt(args[5]));
		if (args.length > 6) {
			bsp.setSplitSize(Integer.valueOf(args[6]));
		}
		if (args.length > 7) {
			bsp.setSendThreshold(Integer.parseInt(args[7]));
		}
		if (args.length > 8) {
			bsp.setSendCombineThreshold(Integer.parseInt(args[8]));
		}
		if (args.length > 9) {
			bsp.setMemoryDataPercent(Float.parseFloat(args[9]));
		}
		if (args.length > 10) {
			bsp.setBeta(Float.parseFloat(args[10]));
		}
		if (args.length > 11) {
			bsp.setHashBucketNumber(Integer.parseInt(args[11]));
		}
		if (args.length > 12) {
			bsp.setMessagePackSize(Integer.parseInt(args[12]));
		}
		
		// Summit the job!
		bsp.waitForCompletion(true);
	}

}