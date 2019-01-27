package com.redislabs.redisgraph;


public interface Statistics {
	
	/**
	 * Different Statistics labels 
	 */
	enum Label{
		LABELS_ADDED("Labels added"),
		INDICES_ADDED("Indices added"),
		NODES_CREATED("Nodes created"),
		NODES_DELETED("Nodes deleted"),
		RELATIONSHIPS_DELETED("Relationships deleted"),
		PROPERTIES_SET("Properties set"),
		RELATIONSHIPS_CREATED("Relationships created"),
		QUERY_INTERNAL_EXECUTION_TIME("Query internal execution time");

	    private final String text;

		Label(String text) {
			this.text = text;
		}
		
		@Override
		public String toString() {
	        return this.text;
	    }

		/**
		 * Get a Label by label text
		 * 
		 * @param value label text
		 * @return the matching Label
		 */
	    public static Label getEnum(String value) {
	        for(Label v : values()) {
	            if(v.toString().equalsIgnoreCase(value)) return v;
	        }
	        return null;
	    }
	}
	
	/**
	 * Retrieves the relevant statistic  
	 * 
	 * @param label the requested statistic label 
	 * @return a String representation of the specific statistic or null
	 */
	String getStringValue(Statistics.Label label);

	int nodesCreated();
	
	int nodesDeleted();
	
	int indicesAdded();
	
	int labelsAdded();
	
	int relationshipsDeleted();
	
	int relationshipsCreated();
	
	int propertiesSet();
}
