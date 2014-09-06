package com.poc.trident.fake.generator;

import java.util.List;
import java.util.Map;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class Utils {

	/**
	 * A filter that filters nothing but prints the tuples it sees. Useful to
	 * test and debug things.
	 */
	@SuppressWarnings({ "serial", "rawtypes" })
	public static class PrintFilter implements Filter {

		private int partitionIndex;

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			this.partitionIndex = context.getPartitionIndex();
		}

		@Override
		public void cleanup() {
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			// System.out.println("tuple ::..... "+tuple);
			System.err.println("I am in partition [" + partitionIndex + "] ");
			List<Object> x = (List<Object>) tuple.getValueByField("batch_out");
			System.out.println("PrintFilter x....... " + x);
			return true;
		}
	}

	@SuppressWarnings("serial")
	public static class PartitionPrint extends BaseFilter {

		private int partitionIndex;

		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			this.partitionIndex = context.getPartitionIndex();
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			System.err.println("I am in partition [" + partitionIndex + "] "
					+ "tuple :" + tuple);
			return true;
		}
	}

	@SuppressWarnings("serial")
	public static class LocationFilter extends BaseFilter {

		private String location;

		public LocationFilter(String location) {
			this.location = location;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map conf, TridentOperationContext context) {

		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			if (tuple.get(1).equals(location)) {
				return false;
			}
			return true;
		}
	}

}
