package com.echat.storm.analysis.operation;

import storm.trident.tuple.TridentTuple;
import storm.trident.operation.CombinerAggregator;
import com.echat.storm.analysis.FieldsConstrants;

public class Counter implements CombinerAggregator<Long> {
	@Override
	public Long init(TridentTuple tuple) {
		return 1L;
	}

	@Override
	public Long combine(Long v1,Long v2) {
		return v1 + v2;
	}

	@Override
	public Long zero() {
		return 0L;
	}
}

