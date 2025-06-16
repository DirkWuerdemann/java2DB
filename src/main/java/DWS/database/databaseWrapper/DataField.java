package DWS.database.databaseWrapper;

/**
 * Copyright 2025 Dirk Wuerdemann
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

import java.sql.Blob;
import java.util.Date;

public class DataField {
	public 	String 			strField=null;
	public	String 			strValue=null;
	public	boolean			bolValue=false;
	public 	double 			douValue=0;
	public 	Date			datValue=null;
	public	Blob			blobValue=null;
	public 	FieldFormat		intFormat;
	
	public DataField( String strField, Blob blobValue ) {
		this.strField=strField;
		this.blobValue=blobValue;
		intFormat=FieldFormat.BLOB;
	}
	public DataField( String strField, boolean bolValue ) {
		this.strField=strField;
		this.bolValue=bolValue;
		intFormat=FieldFormat.BOOLEAN;
	}	
	public DataField( String strField, Date datValue ) {
		this.strField=strField;
		this.datValue=datValue;
		intFormat=FieldFormat.DATE;
	}
	public DataField( String strField, double douValue ) {
		this.strField=strField;
		this.douValue=douValue;
		intFormat=FieldFormat.NUMBER;
	}
	public DataField( String strField, String strValue ) {
		this.strField=strField;
		/*
		 * In string for SQL the ' and "" needs to be masked
		 */
		if (strValue != null ) {
			this.strValue=strValue.replaceAll("'", "''");
			this.strValue=this.strValue.replaceAll("\"", "\"\"");
		} else {
			this.strValue=strValue;
		}
		intFormat=FieldFormat.TEXT;
	}
}
/*
 * Formats
 */
//enum 			enFormat { BOOLEAN, DATE, NUMBER, TEXT};