package com.falkordb.impl.resultset;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.falkordb.Statistics;

import redis.clients.jedis.util.SafeEncoder;

/**
 * Query result statistics interface implementation
 */
public class StatisticsImpl implements Statistics  {
	//members
	private final List<byte[]> raw;
	private final Map<Statistics.Label, String> statistics;

	/**
	 * A raw representation of query execution statistics is a list of strings
	 * (byte arrays which need to be de-serialized).
	 * Each string is built in the form of "K:V" where K is statistics label and V is its value.
	 * @param raw a raw representation of the query execution statistics
	 */
	public StatisticsImpl(List<byte[]> raw){
		this.raw = raw;
		this.statistics = new EnumMap<>(Statistics.Label.class); // lazy loaded
	}


	/**
	 *
	 * @param label the requested statistic label as key
	 * @return a string with the value, if key exists, null otherwise
	 */
	@Override
	public String getStringValue(Statistics.Label label) {
		return getStatistics().get(label);
	}
	
	private Map<Statistics.Label, String> getStatistics(){
		if(statistics.size() == 0) {		
			for(byte[]  tuple :  this.raw) {
			    String text = SafeEncoder.encode(tuple);
				String[] rowTuple = text.split(":");
				if(rowTuple.length == 2) {
				  Statistics.Label label = Statistics.Label.getEnum(rowTuple[0]);
				  if(label != null) {
				    this.statistics.put( label, rowTuple[1].trim());
				  }
				} 
			}
		}
		return statistics;
	}

	/**
	 *
	 * @param label the requested statistic label as key
	 * @return a string with the value, if key exists, 0 otherwise
	 */
	public int getIntValue(Statistics.Label label) {
		String value = getStringValue(label);
		return value==null ? 0 : Integer.parseInt(value);
	}

	/**
	 *
	 * @return number of nodes created after query execution
	 */
	@Override
	public int nodesCreated() {
		return getIntValue(Label.NODES_CREATED);
	}

	/**
	 *
	 * @return number of nodes deleted after query execution
	 */
	@Override
	public int nodesDeleted() {
		return getIntValue(Label.NODES_DELETED);
	}

	/**
	 *
	 * @return number of indices added after query execution
	 */
   @Override
   public int indicesAdded() {
        return getIntValue(Label.INDICES_ADDED);
   }


	@Override
	public int indicesDeleted() {return getIntValue(Label.INDICES_DELETED);}

	/**
	 *
	 * @return number of labels added after query execution
	 */
	@Override
	public int labelsAdded() {
		return getIntValue(Label.LABELS_ADDED);
	}

	/**
	 *
	 * @return number of relationship deleted after query execution
	 */
	@Override
	public int relationshipsDeleted() {
		return getIntValue(Label.RELATIONSHIPS_DELETED);
	}

	/**
	 *
	 * @return number of relationship created after query execution
	 */
	@Override
	public int relationshipsCreated() {
		return getIntValue(Label.RELATIONSHIPS_CREATED);
	}

	/**
	 *
	 * @return number of properties set after query execution
	 */
	@Override
	public int propertiesSet() {
		return getIntValue(Label.PROPERTIES_SET);
	}

	/**
	 *
	 * @return The execution plan was cached on Graph.
	 */
	@Override
	public boolean cachedExecution() {
		return getIntValue(Label.CACHED_EXECUTION) == 1;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof StatisticsImpl)) return false;
		StatisticsImpl that = (StatisticsImpl) o;
		return Objects.equals(raw, that.raw) &&
				Objects.equals(getStatistics(), that.getStatistics());
	}

	@Override
	public int hashCode() {
		return Objects.hash(raw, getStatistics());
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("StatisticsImpl{");
		sb.append("statistics=").append(getStatistics());
		sb.append('}');
		return sb.toString();
	}
}
