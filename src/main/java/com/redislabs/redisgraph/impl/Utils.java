package com.redislabs.redisgraph.impl;

import org.apache.commons.text.translate.AggregateTranslator;
import org.apache.commons.text.translate.CharSequenceTranslator;
import org.apache.commons.text.translate.LookupTranslator;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Utilities class
 */
public class Utils {
    public static final List<String> DUMMY_LIST = new ArrayList<>(0);
    public static final Map<String, List<String>> DUMMY_MAP = new HashMap<>(0);
    public static final String COMPACT_STRING = "--COMPACT";
    public static final String TIMEOUT_STRING = "TIMEOUT";

    private static final CharSequenceTranslator ESCAPE_CHYPER;
    static {
        final Map<CharSequence, CharSequence> escapeJavaMap = new HashMap<>();
        escapeJavaMap.put("\'", "\\'");
        escapeJavaMap.put("\"", "\\\"");
        ESCAPE_CHYPER = new AggregateTranslator(new LookupTranslator(Collections.unmodifiableMap(escapeJavaMap)));
    }
    
    private Utils() {}

    /**
     *
     * @param str - a string
     * @return the input string surrounded with quotation marks, if needed
     */
    private static String quoteString(String str){
        StringBuilder sb = new StringBuilder(str.length()+2);
        sb.append('"');
        sb.append(str.replace("\"","\\\""));
        sb.append('"');
        return sb.toString();
    }


    /**
     * Prepare and formats a query and query arguments
     * @param query - query
     * @param args - query arguments
     * @return formatted query
     * @deprecated use {@link #prepareQuery(String, Map)} instead.
     */
    @Deprecated
    public static String prepareQuery(String query, Object ...args){
        if(args.length > 0) {
            for(int i=0; i<args.length; ++i) {
                if(args[i] instanceof String) {
                    args[i] = "\'" + ESCAPE_CHYPER.translate((String)args[i]) + "\'";
                }
            }
            query = String.format(query, args);
        }
        return query;
    }

    /**
     * Prepare and formats a query and query arguments
     * @param query - query
     * @param params - query parameters
     * @return query with parameters header
     */
    public static String prepareQuery(String query, Map<String, Object> params){
        StringBuilder sb = new StringBuilder("CYPHER ");
        for(Map.Entry<String, Object> entry : params.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            sb.append(key).append('=');
            sb.append(valueToString(value));
            sb.append(' ');
        }
        sb.append(query);
        return sb.toString();
    }

    private static String arrayToString(Object[] arr) {
        StringBuilder sb = new StringBuilder().append('[');
        sb.append(String.join(", ", Arrays.stream(arr).map(Utils::valueToString).collect(Collectors.toList())));
        sb.append(']');
        return sb.toString();
    }

    private static String valueToString(Object value) {
        if(value == null)
            return "null";
        
        if(value instanceof String){
            return quoteString((String) value);
        }
        if(value instanceof Character){
            return quoteString(((Character)value).toString());
        }

        if(value instanceof Object[]){
            return arrayToString((Object[]) value);

        }
        if(value instanceof List){
            @SuppressWarnings("unchecked")
			List<Object> list = (List<Object>)value;
            return arrayToString(list.toArray());
        }
        return value.toString();
    }

    /**
     * Prepare and format a procedure call and its arguments
     * @param procedure - procedure to invoke
     * @param args - procedure arguments
     * @param kwargs - procedure output arguments
     * @return formatter procedure call
     */
    public static String prepareProcedure(String procedure, List<String> args  , Map<String, List<String>> kwargs){
        args = args.stream().map( Utils::quoteString).collect(Collectors.toList());
        StringBuilder queryStringBuilder =  new StringBuilder();
        queryStringBuilder.append("CALL ").append(procedure).append('(');
        int i = 0;
        for (; i < args.size() - 1; i++) {
            queryStringBuilder.append(args.get(i)).append(',');
        }
        if (i == args.size()-1) {
            queryStringBuilder.append(args.get(i));
        }
        queryStringBuilder.append(')');
        List<String> kwargsList = kwargs.getOrDefault("y", null);
        if(kwargsList != null){
            i = 0;
            for (; i < kwargsList.size() - 1; i++) {
                queryStringBuilder.append(kwargsList.get(i)).append(',');

            }
            queryStringBuilder.append(kwargsList.get(i));
        }
        return queryStringBuilder.toString();
    }
}
