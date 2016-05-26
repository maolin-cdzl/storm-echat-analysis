package com.echat.storm.analysis.state;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;

import backtype.storm.tuple.Values;
import backtype.storm.task.IMetricsContext;
import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;

public class HashMapState<T> implements IBackingMap<T> {
	private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = new HashMap<StateType, Serializer>() {{
        put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }};

	public static class Options<T> implements Serializable {
		public String globalKey = "$GLOBAL$";
		public Serializer<T> serializer = null;
	}

	public static StateFactory opaque() {
		return opaque(new Options());
	}

	public static StateFactory opaque(Options<OpaqueValue> opts) {
		return new Factory(StateType.OPAQUE,opts);
	}

	public static StateFactory transactional() {
		return transactional(new Options());
	}
	public static StateFactory transactional(Options<TransactionalValue> opts) {
		return new Factory(StateType.TRANSACTIONAL,opts);
	}

	public static StateFactory nonTransactional() {
		return nonTransactional(new Options());
	}
	public static StateFactory nonTransactional(Options<Object> opts) {
		return new Factory(StateType.NON_TRANSACTIONAL,opts);
	}

	protected static class Factory implements StateFactory {
		private StateType _type;
		private Options _opts;
		private Serializer _ser;

		public Factory(StateType type,Options opts) {
			_type = type;
			_opts = opts;

			if(opts.serializer==null) {
                _ser = DEFAULT_SERIALZERS.get(type);
                if(_ser==null) {
                    throw new RuntimeException("Couldn't find serializer for state type: " + type);
                }
            } else {
                _ser = opts.serializer;
            }
		}

		@Override
		public State makeState(Map conf, IMetricsContext context, int partitionIndex, int numPartitions) {
			HashMapState s = new HashMapState(_opts,_ser);
			MapState ms;

			if(_type == StateType.NON_TRANSACTIONAL) {
                ms = NonTransactionalMap.build(s);
            } else if(_type==StateType.OPAQUE) {
                ms = OpaqueMap.build(s);
            } else if(_type==StateType.TRANSACTIONAL){
                ms = TransactionalMap.build(s);
            } else {
                throw new RuntimeException("Unknown state type: " + _type);
            }
            return new SnapshottableMap(ms, new Values());
		}
	}

	private Options			_opts;
	private Serializer		_ser;
	private HashMap<String,T>		_map;

	public HashMapState(Options opts,Serializer<T> ser) {
		_opts = opts;
		_ser = ser;
	}

	@Override
	public List<T> multiGet(List<List<Object>> keys) {
		List<T> ret = new ArrayList(keys.size());
		for(List<Object> key: keys) {
			ret.add( _map.get(toSingleKey(key)) );
		}
		return ret;
	}

	@Override
	public void multiPut(List<List<Object>> keys,List<T> vals) {
		for(int i=0; i < keys.size(); i++) {
			_map.put(toSingleKey(keys.get(i)),vals.get(i));
		}
	}

	private String toSingleKey(List<Object> keys) {
		if( keys.size() == 1 ) {
			return keys.get(0).toString();
		} else if( keys.size() < 1 ) {
			throw new RuntimeException("Memcached state does not support compound keys");
		} else {
			String key = new String();
			for(Object o : keys) {
				key = key + o.toString();
			}
			return key;
		}
	}
}

