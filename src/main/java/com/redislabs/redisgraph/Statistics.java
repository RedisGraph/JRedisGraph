package com.redislabs.redisgraph;

public interface Statistics {
	enum Label{
		LABELS_ADDED("Labels added"),
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

	    public static Label getEnum(String value) {
	        for(Label v : values()) {
	            if(v.toString().equalsIgnoreCase(value)) return v;
	        }
	        throw new IllegalArgumentException();
	    }
	}
	
	String getStringValue(Statistics.Label label);
}
