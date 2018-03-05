package com.tiaa.esp.hadoop.model.schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class DataSchema {

	public static final int STRING_TYPE = 0;
	public static final int DECIMAL_TYPE = 1;
	public static final int DATE_TYPE = 2;
	public static final int INTEGER_TYPE = 3;
	public static final String SCHEMA_REGEX = "([A-Za-z0-9_]+)\\s*\\:(nullable)?\\s*(string|decimal|date|int64|int32)\\s*(\\[([a-z=0-9,]+)\\])?\\s*(\\{(['%\\-A-Za-z0-9,=_ ]+)\\})?";
	public static final String ATTRIBUTES_REGEX = "([a-z_]+)(='?([A-Za-z0-9\\-,%]*)'?)?";
	public static final String ATTRIBUTES_REGEX_NG = "packed|unsigned|zoned|[a-z_]+='[A-Za-z0-9\\-,%]*'|[a-z_]+=[A-Za-z0-9\\-%]*";
	
	public static final Map<String, Integer> DATATYPE_MAP;
    static {
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("date", DATE_TYPE);
        map.put("decimal", DECIMAL_TYPE);
        map.put("string", STRING_TYPE);
        map.put("int64", INTEGER_TYPE);
        map.put("int32", INTEGER_TYPE);
        DATATYPE_MAP = Collections.unmodifiableMap(map);
    }
    
    // parses and sets attributes
    public abstract void setAttributes(String attributes, boolean nullable);
    
	// name of column
	public abstract String getColumnName();
	
	// sets column name
	public abstract void setColumnName(String columnName);
	
	// default value
	public abstract String getDefaultValue();
	
	// get length
	public abstract int getLength();
	
	// read length
	public abstract void setLength(String length);
	
	// data type code
	public abstract int getType();

	// date type
	public abstract String getTypeName();
	
	// is field nullable or mandatory
	public abstract boolean isNullable();
}
