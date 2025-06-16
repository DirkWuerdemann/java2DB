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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;


/*
 * Used in XML2Database. Maybe some rework needed
 */
public class TableFields {

	private	int				intColumnCount			=	0;
	private	String			strColumnName[]			= 	null;
	private	int				intColumnType[]			= 	null;
	private String			strColumnTypeName[]		=	null;
	private int				intColumnSize[]	=	null;	
	private String			strDefaultValue[]		=	null;
	private int				intColumnDisplaySize[]	=	null;
	private int				intPrecision[]			=	null;
	private int				intScale[]				=	null;
	private int				intNullable[]			=	null;
	private String			strTableName[]			=	null;
	private String			strSchemaName[]			=	null;
	private String			strCatalogName[]		=	null;
	private boolean			bolPrimaryKey[]			=	null;
	private boolean			bolAutoIncrement[]		=	null;
	private	long			lngResult				=	0;

	/*
	 * 65209kkk
	 */
	public TableFields( ResultSet rs ) {
		
		int						i						=	0;
		ResultSetMetaData		rsmd 					= 	null;
		ResultSet				rsColumns				=	null;
		try {			
			
			/*
			 * Get the meta data
			 */
    		rsmd=rs.getMetaData();
    		if(rsmd==null) {
    			/*
    			 * Error
    			 */
    			lngResult=-65209002;
    			return;
    		}
    		/*
    		 * Get Table Size
    		 */
    		intColumnCount=rsmd.getColumnCount();
    		/*
    		 * size the arrays. Take +1 just to be safe :-)
    		 */
    		strColumnName 			= 	new String[intColumnCount+1];
       		intColumnType 			= 	new int[intColumnCount+1];
       		strColumnTypeName	 	=	new String[intColumnCount+1];
       		strDefaultValue			=	new String[intColumnCount+1];
       		intColumnDisplaySize	= 	new int[intColumnCount+1];
       		intPrecision			= 	new int[intColumnCount+1];
       		intScale				=	new int[intColumnCount+1];
       		intNullable				=	new int[intColumnCount+1];
       		strTableName			= 	new String[intColumnCount+1];
       		strSchemaName			= 	new String[intColumnCount+1];
       		strCatalogName			= 	new String[intColumnCount+1];
       		bolPrimaryKey			=	new boolean[intColumnCount+1];
       		bolAutoIncrement		=	new boolean[intColumnCount+1];	
    		/*
    		 * Loop over all columns
    		 */
    		for(i=0 ; i<intColumnCount; i++){
    			strColumnName[i]		=	rsmd.getColumnName(i+1);
    			intColumnType[i]		=	rsmd.getColumnType(i+1);
    			strColumnTypeName[i]	=	rsmd.getColumnTypeName(i+1);
    			intColumnDisplaySize[i]	=	rsmd.getColumnDisplaySize(i+1);
    			intPrecision[i]			=	rsmd.getPrecision(i+1);
    			intScale[i]				=	rsmd.getScale(i+1);
    			intNullable[i]			=	rsmd.isNullable(i+1);
    			strTableName[i]			=	rsmd.getTableName(i+1);
    			strSchemaName[i]		=	rsmd.getSchemaName(i+1);
    			strCatalogName[i]		=	rsmd.getCatalogName(i+1);
    			//strDefaultValue[i]		=	rsmd.col
    			
    			/*
    			 * Check and set autoIncrement
    			 */
    			//bolAutoIncrement[i] 	=	isColumnSerial(strDatabaseProductName, rsmd);
    		}
    		return;
    	} catch (Exception e) {
    		lngResult=-65209001;
    		System.err.println(e.getMessage());
			return;
    	}
		
	}
	/*
	 * 65209kkk
	 */
	public TableFields( Db database, String strTable ) {
		
		int						i						=	0;
		int						intPkFlag				=	0;
		int						intPkColumn				=	-1;
		DatabaseMetaData 		dbSourceMetaData		=	null;		
		ResultSetMetaData		rsmd 					= 	null;
		ResultSetMetaData		rsmdPK 					= 	null;
		ResultSet				rsColumns				=	null;
		ResultSet				rs						=	null;
		ResultSet				rsPK					=	null;
		String					strSql					=	null;
		String					strPK					=	null;
		
		try {
			/*
			 * Open the table
			 */
       		strSql = "SELECT * FROM "+strTable;
			rs = database.querySQL(strSql);	
			if( rs == null) {
				/*
				 * Error
				 */
    			lngResult=-65209003;
    			return ;
			}
    		rsmd=rs.getMetaData();
    		if(rsmd==null) {
    			/*
    			 * Error
    			 */
    			lngResult=-65209002;
    			return;
    		}
    		/*
    		 * Get Table Size
    		 */
    		intColumnCount=rsmd.getColumnCount();
    		/*
    		 * size the arrays. Take +1 just to be safe :-)
    		 */
    		strColumnName 			= 	new String[intColumnCount];
       		intColumnType 			= 	new int[intColumnCount];
       		strColumnTypeName	 	=	new String[intColumnCount];
       		strDefaultValue			=	new String[intColumnCount];
       		intColumnDisplaySize	= 	new int[intColumnCount];
       		intColumnSize			= 	new int[intColumnCount];
       		intPrecision			= 	new int[intColumnCount];
       		intScale				=	new int[intColumnCount];
       		intNullable				=	new int[intColumnCount];
       		strTableName			= 	new String[intColumnCount];
       		strSchemaName			= 	new String[intColumnCount];
       		strCatalogName			= 	new String[intColumnCount];
       		bolPrimaryKey			=	new boolean[intColumnCount];
       		bolAutoIncrement		=	new boolean[intColumnCount];
       		
       		/*
       		 * Get the columns
       		 */
       		String strDatabaseProductName  = database.getDataBaseProductName();
       		dbSourceMetaData=database.getMetaData();
			
			switch(strDatabaseProductName.toUpperCase()) {
			
				case "MARIADB":
				case "MYSQL":				
					rsColumns = dbSourceMetaData.getColumns( null,database.getDatabaseName(), strTable, null);
					/*
					 * Test
					
					i=0;
					while (rsColumns.next() ){
						System.out.println(i+" "+ rsColumns.getString("COLUMN_NAME"));
						i++;
					}
					rsColumns.close();
					rsColumns = dbSourceMetaData.getColumns( null,database.getDatabaseName(), strTable, null);
					*/
					break;
				case "POSTGRESQL":
					rsColumns = dbSourceMetaData.getColumns(null, "public", strTable.toLowerCase(), null);					
					break;
				default:
					rsColumns = dbSourceMetaData.getColumns(null, null, strTable, null);					
					break;
			}
    		/*
    		 * Loop over all columns
    		 * 
    		 * 2025-05-29: Error in MariaDB. rsColummns has more columns. MariaDB runs 2 times through the list
    		 */			
			i=0;
			while (rsColumns.next() && i < intColumnCount) {
    			strColumnName[i]		=	rsColumns.getString("COLUMN_NAME");
    			intColumnType[i]		=	rsColumns.getInt("DATA_TYPE");
    			strColumnTypeName[i]	=	rsColumns.getString("TYPE_NAME");
    			intColumnSize[i]		=	rsColumns.getInt("COLUMN_SIZE");;
    			intPrecision[i]			=	rsColumns.getInt("DECIMAL_DIGITS");
    			intScale[i]				=	rsmd.getScale(i+1);    			
    			intNullable[i]			=	rsColumns.getInt("NULLABLE");
    			strTableName[i]			=	rsColumns.getString("TABLE_NAME");
    			strSchemaName[i]		=	rsColumns.getString("TABLE_SCHEM");
    			strCatalogName[i]		=	rsColumns.getString("TABLE_CAT");
    			/*
    			 * In postgres in default we have "::" which is not part of the value
    			 */
    			switch(strDatabaseProductName.toUpperCase()) {
    				case "POSTGRESQL":
    					String strHelp = rsColumns.getString("COLUMN_DEF");
    					if( strHelp != null) {
    						String strFields[] 		= strHelp.split("::");
    						strDefaultValue[i]		= strFields[0];
    					}
    					break;
    				default:
    					strDefaultValue[i]		=	rsColumns.getString("COLUMN_DEF");
    	    			break;		
    			}
    			/*
    			 * Sometimes I have the default value NULL and at the sametim NULL is not allowed ( comes from postgres )
    			 * In this case I delete the default value
    			 */
    			if( strDefaultValue[i] != null && strDefaultValue[i].equals("NULL") && intNullable[i] == 0 ) {
    				strDefaultValue[i]	=	null;
    			}
    			/*
    			 * Set initila primary key to false
    			 */
    			bolPrimaryKey[i]		=	false;
    			
    			/*
    			 * Check and set autoIncrement
    			 */
    			bolAutoIncrement[i] 	=	isColumnSerial(database, strDatabaseProductName, rsColumns);
    			if(bolAutoIncrement[i]) {
    				bolPrimaryKey[i] = false;
    			}
    			
    			i++;
    		}
			rsColumns.close();
			/*
			 * get the primary key
			 */
			switch ( strDatabaseProductName.toUpperCase()) {
				case "MARIADB":
				case "MYSQL":
					rsPK = dbSourceMetaData.getPrimaryKeys(null, database.getDatabaseName(), strTable);
					rsmdPK=rsPK.getMetaData();
					while (rsPK.next()) {
						strPK=rsPK.getString("COLUMN_NAME");
						/*
						 * Now search the column
						 */
						intPkColumn =  findTableColumn( strPK );
						if( intPkColumn >= 0) {
							bolPrimaryKey[intPkColumn]=true;
						} else {
							/*
							 * Error: PK Column is not in the table
							 */
			    			lngResult=-65209003;
			    			return;
						}
					}
					rsPK.close();
					break;
				case "SQLITE" :
					strSql="pragma table_info('"+strTable+"');";
					rsPK = database.querySQL(strSql);
					rsmdPK=rsPK.getMetaData();
					while (rsPK.next()) {
						intPkFlag=rsPK.getInt("pk");
						if(intPkFlag!=0) {
							strPK=rsPK.getString("name");
							/*
							 * Now search the column
							 */
							intPkColumn =  findTableColumn( strPK );
							if( intPkColumn >= 0) {
								bolPrimaryKey[intPkColumn]=true;
							} else {
								/*
								 * Error: PK Column is not in the table
								 */
				    			lngResult=-65209003;
				    			rsPK.close();
				    			return;
							}
						}
					}
					rsPK.close();
					break;
				default:
					rsPK = dbSourceMetaData.getPrimaryKeys(null, null, strTable);
					rsmdPK=rsPK.getMetaData();
					while (rsPK.next()) {
						strPK=rsPK.getString("COLUMN_NAME");
						/*
						 * Now search the column
						 */
						intPkColumn =  findTableColumn( strPK );
						if( intPkColumn >= 0) {
							bolPrimaryKey[intPkColumn]=true;
						} else {
							/*
							 * Error: PK Column is not in the table
							 */
			    			lngResult=-65209003;
			    			rsPK.close();
			    			return;
						}
					}
					rsPK.close();
					break;
			}
    	} catch (Exception e) {
    		lngResult=-65209001;
    		System.err.println(e.getMessage());
			return;
    	}		
	}
	public int findTableColumn( String strColumnName ) {
		
		int		i	= -1;
		try {
			/*
			 * For postgres all lower case
			 */
			for(i=0 ; i<intColumnCount; i++){
				if(strColumnName.equals(this.strColumnName[i])) {
					/*
					 * Found
					 */
					return(i);
				}
			}
			return(-1);
    	} catch (Exception e) {
    		lngResult=-65209001;
    		System.err.println(e.getMessage());
			return(-65209001);
    	}
	}
	public long getResult() {
		return(lngResult);
	}
	public String getColumnName( int i ) {
		if( i < intColumnCount) {
			return strColumnName[i];
		} else {
			/*
			 * i is to big, array is not of that size
			 */
			lngResult=-65209100;
			return(null);
		}
	}
	public int getColumnType( int i ) {
		if( i < intColumnCount) {
			return intColumnType[i];
		} else {
			/*
			 * i is to big, array is not of that size
			 */
			lngResult=-65209100;
			return(-65209100);
		}
	}
	public String getColumnTypeName( int i ) {
		if( i < intColumnCount) {
			return strColumnTypeName[i];
		} else {
			/*
			 * i is to big, array is not of that size
			 */
			lngResult=-65209100;
			return(null);
		}
	}
	public int getColumnSize( int i ) {
		if( i < intColumnCount) {
			return intColumnSize[i];
		} else {
			/*
			 * i is to big, array is not of that size
			 */
			lngResult=-65209100;
			return(-65209100);
		}
	}
	public int getColumnDisplaySize( int i ) {
		if( i < intColumnCount) {
			return intColumnDisplaySize[i];
		} else {
			/*
			 * i is to big, array is not of that size
			 */
			lngResult=-65209100;
			return(-65209100);
		}
	}
	public int getPrecision( int i ) {
		if( i < intColumnCount) {
			return intPrecision[i];
		} else {
			/*
			 * i is to big, array is not of that size
			 */
			lngResult=-65209100;
			return(-65209100);
		}
	}
	public int getScale( int i ) {
		if( i < intColumnCount) {
			return intScale[i];
		} else {
			/*
			 * i is to big, array is not of that size
			 */
			lngResult=-65209100;
			return(-65209100);
		}
	}
	public String getTableName( int i ) {
		if( i < intColumnCount) {
			return strTableName[i];
		} else {
			/*
			 * i is to big, array is not of that size
			 */
			lngResult=-65209100;
			return(null);
		}
	}
	public String getSchemaName( int i ) {
		if( i < intColumnCount) {
			return strSchemaName[i];
		} else {
			/*
			 * i is to big, array is not of that size
			 */
			lngResult=-65209100;
			return(null);
		}
	}
	public String getCatalogName( int i ) {
		if( i < intColumnCount) {
			return strCatalogName[i];
		} else {
			/*
			 * i is to big, array is not of that size
			 */
			lngResult=-65209100;
			return(null);
		}
	}
	public boolean getPrimaryKeyFlag( int i ) {
		if( i < intColumnCount) {
			return bolPrimaryKey[i];
		} else {
			/*
			 * i is to big, array is not of that size
			 */
			lngResult=-65209100;
			return(false);
		}
	}
	public int getIsNullable( int i ) {
		if( i < intColumnCount) {
			return intNullable[i];
		} else {
			/*
			 * i is to big, array is not of that size
			 */
			lngResult=-65209100;
			return(-1);
		}
	}
	public String getDefaultValue( int i ) {
		if( i < intColumnCount) {
			return strDefaultValue[i];
		} else {
			/*
			 * i is to big, array is not of that size
			 */
			lngResult=-65209100;
			return(null);
		}
	}
	public boolean getAutoIncrement( int i ) {
		if( i < intColumnCount) {
			return bolAutoIncrement[i];
		} else {
			/*
			 * i is to big, array is not of that size
			 */
			lngResult=-65209100;
			return(false);
		}
	}
	public int	getColumnCount() {
		return(intColumnCount);
	}
	/*
	 * 65493kkk
	 * 
	 * no tested jet
	 */
	private	boolean isColumnSerial( Db database, String strDatabaseProductName, ResultSet	rsColumns) {
		
		boolean 					bolResult						=	false;
		String 						strIsAutoIncrement				=	null;
		String						strSql							=	null;
		String						strTableName					=	null;
		String						strColumnName					=	null;
		try {
			switch ( strDatabaseProductName.toUpperCase()) {
				case "MARIADB":
				case "MYSQL":
					strIsAutoIncrement = rsColumns.getString("IS_AUTOINCREMENT");
					if(strIsAutoIncrement.toUpperCase().equals("YES") ) {
						return(true);
					} else {
						return(false);
					}
				case "SQLITE":
					strTableName = rsColumns.getString("TABLE_NAME"); 
					strColumnName =	rsColumns.getString("COLUMN_NAME");
					strSql = "SELECT sql FROM sqlite_master WHERE type='table' AND name='" + strTableName + "'";
					ResultSet rsCreate = database.querySQL(strSql);
			        if (rsCreate.next()) {
			            String createSQL = rsCreate.getString("sql");
			            if (createSQL != null) {
			                // nach AUTOINCREMENT-Muster suchen
			                String normalized = createSQL.toUpperCase().replaceAll("[\\s`\"]+", " ");
			                bolResult	= normalized.matches(".*\\b" + strColumnName.toUpperCase() + "\\b.*PRIMARY KEY.*AUTOINCREMENT.*");
			                return bolResult;
			            }
			        }
					break;
				case "POSTGRESQL":
					strTableName = rsColumns.getString("TABLE_NAME"); 
					strColumnName =	rsColumns.getString("COLUMN_NAME");
					strSql = "SELECT pg_get_serial_sequence('" + strTableName + "', '" + strColumnName + "')";
					ResultSet rs = database.querySQL(strSql);
		            if (rs.next()) {
		                String sequenceName = rs.getString(1);
		                bolResult = sequenceName != null;
		                return bolResult;
		            }
					break;
			}
			/*
			 * done
			 */
			return(false);
    	} catch (Exception e) {
    		lngResult=-65493001;
    		System.err.println(e.getMessage());
			return(false);
    	}
	}
}

