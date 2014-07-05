package com.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner<String> {

	public SimplePartitioner(VerifiableProperties config){
		
	}
	@Override
	public int partition(String key, int numPartitions) {
		int partition = 0;
		int iKey = Integer.parseInt(key);
		if(iKey > numPartitions){
			partition = iKey % numPartitions;
		}
		return partition;
	}

}
