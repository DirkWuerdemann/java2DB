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

public class TableField {
	private 	String 					strField		=	null;			// equals Column Name
	private 	TableFieldCreateFormat	enCreateFormat;
	private		boolean					bolPrimaryKey	=	false;
	private		boolean					bolNotNull		=	false;
	private		boolean					bolDefault		=	false;
	private		String					strDefaultValue	=	null;
	private		int						intSize			=	-1;
	private		String					strCreateCmd	=	null;
	private 	long					lngResult 		= 	0;
	
	public		TableField( String 					strField,
							TableFieldCreateFormat	enCreateFormat ) {
		
		this.strField		=	strField;
		this.enCreateFormat	=	enCreateFormat;		
	}
	
	public		TableField( String 					strField,
							TableFieldCreateFormat	enCreateFormat,
							boolean					bolPrimaryKey,
							boolean					bolNotNull,
							boolean					bolDefault,
							String					strDefaultValue) {

		this.strField		=	strField;
		this.enCreateFormat	=	enCreateFormat;	
		this.bolPrimaryKey	=	bolPrimaryKey;
		this.bolNotNull		=	bolNotNull;
		this.bolDefault		=	bolDefault;
		this.strDefaultValue=	strDefaultValue;
	}
	public		TableField( String 					strField,
							TableFieldCreateFormat	enCreateFormat,
							boolean					bolPrimaryKey,
							boolean					bolNotNull,
							boolean					bolDefault,
							String					strDefaultValue,
							int						intSize) {

		this.strField		=	strField;
		this.enCreateFormat	=	enCreateFormat;	
		this.bolPrimaryKey	=	bolPrimaryKey;
		this.bolNotNull		=	bolNotNull;
		this.bolDefault		=	bolDefault;
		this.strDefaultValue=	strDefaultValue;
		this.intSize		=	intSize;
	}
	public void setPrimaryKeyFlag( boolean bolPrimaryKey ) {
		this.bolPrimaryKey = bolPrimaryKey;
	}
	/*
	 * 65315kkk
	 */
	public String	createCmd(String strDatabaseType) {
		
		/*
		 * Get the format
		 */
		switch(strDatabaseType.toUpperCase()) {
			case "MARIADB":
				strCreateCmd=createMariaDbCmd(strDatabaseType);
				return(strCreateCmd);
			case "MYSQL":
				strCreateCmd=createMySqlCmd(strDatabaseType);				
				return(strCreateCmd);
			case "SQLITE":
				strCreateCmd=createSqliteCmd(strDatabaseType);
				return(strCreateCmd);
			case "POSTGRESQL":
				strCreateCmd=createPostgreSqlCmd(strDatabaseType);
				return(strCreateCmd);
			default:
				/*
				 * Not supported, normally this could not happen
				 */
				lngResult=-65315002;
				return(null);
		}		
	}
	/*
	 * 65316kkk
	 */
	private String createSqliteCmd(String strDatabaseType) {
		
		String				strFormat		=	null;
		
		/*
		 * start the command
		 */
		strCreateCmd = "`" + strField + "`" ;
		/*
		 * Format
		 */
		switch(enCreateFormat) {
			case BIGINT:
				strCreateCmd += " BIGINT ";				
				break;
			case BLOB:
				strCreateCmd += " BLOB ";				
				break;				
			case BOOLEAN:
				strCreateCmd += " BOOLEAN ";				
				break;
			case DATE:
				strCreateCmd += " DATETIME ";				
				break;				
			case DOUBLE:
				strCreateCmd += " DOUBLE ";				
				break;
			case INTEGER:
				strCreateCmd += " INTEGER ";				
				break;
			case SERIAL:
				strCreateCmd += " INTEGER PRIMARY KEY AUTOINCREMENT ";
				/*
				 * No default and no not null statement --> must be tested
				 */
				bolDefault		=	false;
				bolNotNull		=	true;
				bolPrimaryKey 	= 	true;
				break;
			case TEXT:
				strCreateCmd += " TEXT ";
				break;
			case VARCHAR:
				if( intSize != -1 ) {
					strCreateCmd += " VARCHAR("+intSize+") ";
				} else {
					strCreateCmd += " VARCHAR(" + ConstantDB.VARCHAR_SIZE + ") ";
				}				
				break;
			default:
				/*
				 * 	Not supported
				 */
				lngResult=-65316002;
				return(null);
		}		
		/*
		 * Default value
		 */
		if(bolDefault) {
			strCreateCmd += " DEFAULT "+strDefaultValue;
		}
		/*
		 * Not null or Primary Key
		 */
		if(bolNotNull || bolPrimaryKey ) {
			strCreateCmd += " NOT NULL";
		}
		return(strCreateCmd);
	}
	/*
	 * 65317kkk
	 */
	private String createMySqlCmd(String strDatabaseType) {
		
		String				strFormat		=	null;
		
		/*
		 * start the command
		 */
		strCreateCmd = "`" + strField + "`" ;
		/*
		 * Format
		 */
		switch(enCreateFormat) {
			case BIGINT:
				strCreateCmd += " BIGINT ";				
				break;
			case BLOB:
				strCreateCmd += " BLOB ";				
				break;	
			case BOOLEAN:
				strCreateCmd += " BOOLEAN ";				
				break;
			case DATE:
				strCreateCmd += " DATETIME ";				
				break;				
			case DOUBLE:
				strCreateCmd += " DOUBLE ";				
				break;
			case INTEGER:
				strCreateCmd += " INTEGER ";				
				break;
			case SERIAL:
				strCreateCmd += " INT AUTO_INCREMENT ";
				/*
				 * No default and no not null statement
				 */
				bolDefault		=	false;
				bolNotNull		=	false;
				bolPrimaryKey 	= 	true;
				break;				
			case TEXT:
				/*
				 * In case of Primary Key I need to switch to varchar(255)
				 */
				if(bolPrimaryKey) {
					strCreateCmd += " VARCHAR(255) ";
				} else {
					strCreateCmd += " TEXT ";
				}
				break;
			case VARCHAR:
				if( intSize != -1 ) {
					strCreateCmd += " VARCHAR("+intSize+") ";
				} else {
					strCreateCmd += " VARCHAR(" + ConstantDB.VARCHAR_SIZE + ") ";
				}
				break;
			default:
				/*
				 * 	Not supported
				 */
				lngResult=-65317002;
				return(null);
		}		
		/*
		 * Default value
		 */
		if(bolDefault) {
			strCreateCmd += " DEFAULT "+strDefaultValue;
		}
		/*
		 * Not null or Primary Key
		 */
		if(bolNotNull || bolPrimaryKey ) {
			strCreateCmd += " NOT NULL";
		}
		return(strCreateCmd);
	}
	/*
	 * 65318kkk
	 */
	private String createPostgreSqlCmd(String strDatabaseType) {
		
		String				strFormat		=	null;
		
		/*
		 * start the command
		 */
		strCreateCmd =  strField;
		/*
		 * Format
		 */
		switch(enCreateFormat) {
			case BIGINT:
				strCreateCmd += " BIGINT ";				
				break;
			case BLOB:
				strCreateCmd += " BLOB ";				
				break;			
			case BOOLEAN:
				strCreateCmd += " BOOLEAN ";				
				break;
			case DATE:
				strCreateCmd += " TIMESTAMP ";				
				break;				
			case DOUBLE:
				strCreateCmd += " DOUBLE PRECISION";				
				break;
			case INTEGER:
				strCreateCmd += " INTEGER ";				
				break;
			case SERIAL:
				strCreateCmd += " SERIAL ";
				/*
				 * manchmal kommt im default nextval('t__log_lngid_seq' Das brauche ich nicht und auch kein not null
				 */
				bolDefault		=	false;
				bolNotNull		=	false;
				bolPrimaryKey 	= 	true;
				break;					
			case TEXT:
				strCreateCmd += " TEXT ";				
				break;
			case VARCHAR:
				if( intSize != -1 ) {
					strCreateCmd += " VARCHAR("+intSize+") ";
				} else {
					strCreateCmd += " VARCHAR(" + ConstantDB.VARCHAR_SIZE + ") ";
				}
				break;
			default:
				/*
				 * 	Not supported
				 */
				lngResult=-65318002;
				return(null);
		}		
		/*
		 * Default value
		 */
		if(bolDefault) {
			strCreateCmd += " DEFAULT "+strDefaultValue;
		}
		/*
		 * Not null or Primary Key
		 */
		if(bolNotNull || bolPrimaryKey ) {
			strCreateCmd += " NOT NULL";
		}
		return(strCreateCmd);
	}

	/*
	 * 65320kkk
	 */
	private String createMariaDbCmd(String strDatabaseType) {
		
		String				strFormat		=	null;
		
		/*
		 * start the command
		 */
		strCreateCmd = "`" + strField + "`" ;
		/*
		 * Format
		 */
		switch(enCreateFormat) {
			case BIGINT:
				strCreateCmd += " BIGINT ";				
				break;
			case BLOB:
				strCreateCmd += " BLOB ";				
				break;				
			case BOOLEAN:
				strCreateCmd += " BOOLEAN ";				
				break;
			case DATE:
				strCreateCmd += " DATETIME ";				
				break;				
			case DOUBLE:
				strCreateCmd += " DOUBLE ";				
				break;
			case INTEGER:
				strCreateCmd += " INTEGER ";				
				break;
			case SERIAL:
				strCreateCmd += " INT AUTO_INCREMENT ";
				/*
				 * No default and no not null statement
				 */
				bolDefault		=	false;
				bolNotNull		=	false;
				bolPrimaryKey 	= 	true;
				break;					
			case TEXT:
				/*
				 * In case of Primary Key I need to switch to varchar(255)
				 */
				if(bolPrimaryKey) {
					strCreateCmd += " VARCHAR(" + ConstantDB.VARCHAR_SIZE + ") ";
				} else {
					strCreateCmd += " TEXT ";
				}
				break;
			case VARCHAR:
				if( intSize != -1 ) {
					strCreateCmd += " VARCHAR("+intSize+") ";
				} else {
					strCreateCmd += " VARCHAR(" + ConstantDB.VARCHAR_SIZE + ") ";
				}
				break;				
			default:
				/*
				 * 	Not supported
				 */
				lngResult=-65320002;
				return(null);
		}		
		/*
		 * Default value
		 */
		if(bolDefault) {
			strCreateCmd += " DEFAULT "+strDefaultValue;
		}
		/*
		 * Not null or Primary Key
		 */
		if(bolNotNull || bolPrimaryKey ) {
			strCreateCmd += " NOT NULL";
		}
		return(strCreateCmd);
	}
	
	public String getCreateCmd() {
		return(strCreateCmd);
	}
	public String getFieldName() {
		return(strField);
	}
	public boolean getPrimaryKeyFlag() {
		return(bolPrimaryKey);
	}
	public TableFieldCreateFormat	getCreateFormat() {
		return(enCreateFormat);
	}
}

