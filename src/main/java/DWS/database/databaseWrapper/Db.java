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

/**
 * 
 * Ich mache das nach dem alten Stiefel und nicht mit prepare Statement.
 * 
 * Datentypen 
 * 
 * 
Die meisten der von mir genannten Datentypen sind in SQLite, PostgreSQL, MariaDB und MySQL verfügbar. Hier ist eine Aufschlüsselung der unterstützten Datentypen in den genannten Datenbankmanagementsystemen:

SQLite:

INTEGER
SMALLINT
BIGINT
DECIMAL / NUMERIC
REAL
DOUBLE PRECISION
CHAR(n)
VARCHAR(n)
TEXT
DATE
TIME
TIMESTAMP
BOOLEAN (SQLite speichert BOOLEAN-Werte als INTEGER 0 oder 1)
BLOB
JSON (ab SQLite 3.9.0)
XML (SQLite unterstützt keine eingebauten XML-Datentypen, kann aber XML-Daten als TEXT speichern)
ARRAY (SQLite unterstützt keine ARRAY-Typen, jedoch können Sie eine Tabelle verwenden, um eine Art von "Array" zu simulieren)

PostgreSQL:
Alle von mir genannten Datentypen werden von PostgreSQL unterstützt.

MariaDB und MySQL:

INTEGER
SMALLINT
BIGINT
DECIMAL / NUMERIC
FLOAT
DOUBLE PRECISION
CHAR(n)
VARCHAR(n)
TEXT
DATE
TIME
TIMESTAMP
BOOLEAN (MariaDB ab Version 10.2 und MySQL ab Version 5.7 unterstützen BOOLEAN, jedoch speichert MySQL BOOLEAN-Werte als TINYINT(1))
BLOB
JSON (MariaDB ab Version 10.2.7 und MySQL ab Version 5.7.8 unterstützen JSON)
XML (MariaDB unterstützt keine eingebauten XML-Datentypen, kann aber XML-Daten als TEXT speichern)
ARRAY (MariaDB und MySQL unterstützen keine eingebauten ARRAY-Typen)
Es ist wichtig zu beachten, dass Datenbankversionen und -konfigurationen variieren können, daher sollten Sie immer die Dokumentation der spezifischen Version Ihrer Datenbank überprüfen, um die genauen unterstützten Datentypen zu überprüfen.
 * 
 */
import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import DWS.utilities.fileUtilities.FileUtilities;

public class Db {
	private String 				strDriver 					= 	"";
    private String 				strUrl 						= 	"";
    private String 				strDatabaseType				=	"";
    private String 				strProductName				=	"";
    private String 				strDataBaseFilename 		= 	"";
    private String 				strHost 					= 	"";
    private String 				strUser 					= 	"";
    private String 				strDatabase 				= 	"";
    private String 				strPassword 				= 	"";
    //private String strPort 								= 	"";
    private String 				strLastSQL					=	"";    
	private String 				strUsername					=	System.getProperty("user.name"); //platform independent ;
	private String 				strHostname					=	"";
	private String 				strProcessname				=	java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
	private int 				intFirst					=	1;
	private boolean				bolIsLogLngIDSerial			=	false;
	private boolean 			bolShowErrorMessageBox		=	false;
	private boolean 			bolPrintErrorMessageBox		=	true;
	//private Label				labelFX_Status				=	null;
	private long				lngResult 					= 	0;
	private int					intResult[]					=	null;
	private String				strSqlErrorState 			= 	null;
	private String				strSqlErrorMessage 			= 	null;
	private String				strDriverVersion 			= 	null;
	private int					intJDBCMajorVersion 		= 	-1;
	private int					intJDBCMinorVersion 		= 	-1;
	private String				strDatabaseProductVersion	=	null;
	
    
/*    
    ResultSet resultSet = null;
    Statement statement = null;
 */
    private Connection connection = null;
    private DatabaseMetaData databaseMetaData=null;
    
    private void cleanUpDB() {
    	/*
    	 * Cleanup all internal static values
    	 */
    	strUrl="";
    	strDriver="";
    	strDatabaseType="";
    	strProductName="";
    	strDataBaseFilename="";
    	strHost = "";
    	strUser = "";
    	strDatabase = "";
    	strPassword = "";
    	//strPort = "";
    	connection=null;
    	databaseMetaData=null;
    	strDriverVersion	=	null;
    	intJDBCMajorVersion=-1;
    	intJDBCMinorVersion=-1;
    	strDatabaseProductVersion=null;
    }
    /*
     * Save the database in the "save" subfolder
     */
    private void backupDB() {
    	
    	
    	switch(strDatabaseType) {
			case "MDB":
			case "SQLITE":
				// i do a file copy here
				File sourceDB= new File(strDataBaseFilename);
				String strDate = ConstantDB.DATEFORMAT.format(new Date());
				String strFileName=sourceDB.getName()+"."+strDate;
				String strAbsolutePath = sourceDB.getAbsolutePath();
				String strPathName=sourceDB.getAbsolutePath().substring(0,strAbsolutePath.lastIndexOf(File.separator))+"\\save\\";
				String strTarget=strPathName + strFileName;
				File targetPath=new File(strPathName);				
				if (!targetPath.exists()){
					// create the dir
					targetPath.mkdir();
				}
				Path sourcePath=Paths.get(strDataBaseFilename);
				Path targetPath1=Paths.get(strTarget);
				try {
					/*
					 * Copy only if file no exist
					 */
					File targetDB= new File(strTarget);
					if (!targetDB.exists()) {
						Files.copy( sourcePath, targetPath1 );
					}
				} catch (Exception e) {
					System.err.println(e.toString());
				}
				break;
			case "MARIADB":
			case "MYSQL":
			case "POSTGRESQL":
			case "TERADATA":
				// nothing done so fare
				break;
		  			
		default:
			/* 
			 * Database not supported so far
			 */
			System.out.println("Database not supported so far [" + strDatabaseType + "]");
			return;
    	}    	
    }
    public String getDatabaseName() {
    	return( strDatabase);
    }
    public String getDatabaseType() {
    	return( strDatabaseType);
    }
    public String getDatabaseProductVersion() {
    	return(strDatabaseProductVersion);
    }
    public long	getLngResult() {
    	return(lngResult);
    }
    public String getSqlErrorState() {
    	return(strSqlErrorState);
    }
    public String getSqlErrorMessage() {
    	return(strSqlErrorMessage);
    }
    public String getDataBaseProductName() {
    	return(strProductName);
    }
    public String getVersion() {
    	return( ConstantDB.VERSION );
    }
    public int[] getIntResult() {
    	return(intResult);
    }
    public Db(String Host, String db, String username, String password) {
        strHost = Host;
        strDatabase = db;
        strUser = username;
        strPassword = password;
        strUrl = "jdbc:mysql://" + strHost + "/" + strDatabase + "?user=" + strUser
                + "&password=" + strPassword;
        //strDriver = ("com.mysql.jdbc.Driver");
        strDriver = ("com.mysql.cj.jdbc.Driver");
        
    }

    public Db(String filePath) {
        strUrl = "jdbc:sqlite:" + new File(filePath).getAbsolutePath();
        strDriver = ("org.sqlite.JDBC");
    }
    public Db() {
    	cleanUpDB();
    }
    public String getLastSQL() {
    	return( strLastSQL);
    }
    public DatabaseMetaData getMetaData() {
    	return(databaseMetaData);
    }
    /*
     * 65057nnn
     * Here I should check if Database exists.
     * 2016-09-12 DW: The driver needs to be set here as well
     */
    public  long initDB(String strDBType, String... args) {
    	//int intResult=0;
    	int intArgc=0;
    	lngResult=0;
    	
    	intArgc=args.length;
 
    	if (intArgc == 0 ){
    		// wrong usage
    		/*
    		 * initDB("SQLite", filename)
    		 * initDB("MySQL", host, database, username, password)
    		 */
    		return -65057001;
    	}
    	// Cleanup class
    	cleanUpDB();
    	
    	/*
    	 * I should implement also a way to set the url via the ini file.
    	 * In that case I would use the replace functionality from XML2Database
    	 */
    	
    	strDatabaseType=strDBType.toUpperCase();
    	switch(strDatabaseType) {
			case "MARIADB":
				/*
				 * 2017-02-04 DW: Add 'zeroDateTimeBehavior=convertToNull' to the URL
				 */
				strHost 	= args[0];
				strDatabase = args[1];
				strUser 	= args[2];
				strPassword = args[3];
				strUrl = "jdbc:mariadb://" + strHost + "/" + strDatabase + "?zeroDateTimeBehavior=convertToNull&user=" + strUser
						+ "&password=" + strPassword;
				strDriver = ("org.mariadb.jdbc.Driver");
				break;
    		case "MDB":
    			// Alternative ucanaccess.sourceforge.net/site.html
    			strDataBaseFilename=args[0];
    			strUrl = "jdbc:ucanaccess://"+new File(strDataBaseFilename).getAbsolutePath();;
    	        strDriver = ("net.ucanaccess.jdbc.UcanaccessDriver");    			
    			break;
    		case "MYSQL":
    			/*
    			 * 2017-02-04 DW: Add 'zeroDateTimeBehavior=convertToNull' to the URL
    			 */
    	        strHost = args[0];
    	        strDatabase = args[1];
    	        strUser = args[2];
    	        strPassword = args[3];
    	        strUrl = "jdbc:mysql://" + strHost + "/" + strDatabase + "?zeroDateTimeBehavior=convertToNull&user=" + strUser
    	                + "&password=" + strPassword;
    	        strDriver = ("com.mysql.cj.jdbc.Driver");
    			break;
    		case "POSTGRESQL":
    	        strHost = args[0];
    	        strDatabase = args[1];
    	        strUser = args[2];
    	        strPassword = args[3];
    	        strUrl = "jdbc:postgresql://" + strHost + "/" + strDatabase + "?user=" + strUser
    	                + "&password=" + strPassword;
    	        strDriver = ("org.postgresql.Driver");    			
    			break;
    		case "SQLITE":
    			/*
    			 * Check if sqlite file exists
    			 * 
    			 * 2021-07-02 DW: Try to elimnate the compound limit by setting it to 0
    			 */
    			if ( FileUtilities.fileExist(args[0])) {
    				strDataBaseFilename=args[0];
    				strUrl = "jdbc:sqlite:" + new File(strDataBaseFilename).getAbsolutePath() +"?limit_compound_select=0";
    				strDriver = ("org.sqlite.JDBC");    	
    			} else {
    				/*
    				 * Data base does not exist :-(
    				 */
    				return( -65057004);
    			}
    			break; 
    		case "TERADATA":
    			/*
    			 * We need an error handling at least of the strUrl
    			 */
    	        strHost = args[0];
    	        strDatabase = args[1];
    	        strUser = args[2];
    	        strPassword = args[3];
    	        strUrl=args[4];
    	        strUrl=strUrl.replace("[Host]", strHost);
    	        strUrl=strUrl.replace("[Database]", strDatabase);
    	        strUrl=strUrl.replace("[User]", strUser);
    	        strUrl=strUrl.replace("[Password]", strPassword);
    	        
    	        //strUrl="jdbc:teradata://"+ strHost + "/database="+strDatabase+",USER="+strUser+",PASSWORD="+strPassword+",TMODE=ANSI,TRUSTED_SQL=ON,CHARSET=UTF8,LOGMECH=LDAP";
    	        //strUrl = "jdbc:teradata://158.92.9.29/TMODE=ANSI,CHARSET=UTF8,LOGMECH=LDAP";
    	        strDriver = ("com.teradata.jdbc.TeraDriver");  
    			break;
    		default:
    			/* 
    			 * Database not supported so far
    			 */
    			System.out.println("Database not supported so far [" + args[1] + "]");
    			return -65057003;
    	}
    	// Backup the data base
    	backupDB();
    	    	
    	return lngResult;
    }
    
    
    public long openConnection() {
        try {
            Class.forName(strDriver);
            //connection = DriverManager.getConnection(strUrl, "Z0006WNK-U01", "BmgbSpnfp1607-");
            
            connection = DriverManager.getConnection(strUrl);

            // Get Metadata
            databaseMetaData 			= 	connection.getMetaData();
            strProductName    			= 	databaseMetaData.getDatabaseProductName();
            strDriverVersion			=	databaseMetaData.getDriverVersion();
            intJDBCMajorVersion			=	databaseMetaData.getJDBCMajorVersion();
            intJDBCMinorVersion			=	databaseMetaData.getJDBCMinorVersion();
            strDatabaseProductVersion	=	databaseMetaData.getDatabaseProductVersion();
            
            return 0;
        } catch (SQLException e) {
        	strSqlErrorState=e.getSQLState();
        	strSqlErrorMessage=e.getMessage();
            System.out.println("Could not connect to Database (MariaDB/MySQL/PostgreSQL/SQLite/Teradata...) server! because: " + e.getMessage());
            return -1;
        } catch (ClassNotFoundException e) {
            System.out.println("JDBC Driver not found!");
            System.out.println("You need to initialize 1st. Call initDB(...)");
            return -2;
        } catch( Exception e ) {
            System.out.println("Other exception ");
            System.out.println(e.getMessage());
            return -3;
        }
    }

    public long openConnectionExclusive() {
        try {
            Class.forName(strDriver);
            //connection = DriverManager.getConnection(strUrl, "Z0006WNK-U01", "BmgbSpnfp1607-");
            
            //String strHelp = strUrl+"?locking_mode=EXCLUSIVE";
            connection = DriverManager.getConnection(strUrl);

            // Get Metadata
            databaseMetaData 			= 	connection.getMetaData();
            strProductName    			= 	databaseMetaData.getDatabaseProductName();
            strDriverVersion			=	databaseMetaData.getDriverVersion();
            intJDBCMajorVersion			=	databaseMetaData.getJDBCMajorVersion();
            intJDBCMinorVersion			=	databaseMetaData.getJDBCMinorVersion();
            strDatabaseProductVersion	=	databaseMetaData.getDatabaseProductVersion();
            
            return 0;
        } catch (SQLException e) {
        	strSqlErrorState=e.getSQLState();
        	strSqlErrorMessage=e.getMessage();
            System.out.println("Could not connect to Database (MariaDB/MySQL/PostgreSQL/SQLite/Teradata...) server! because: " + e.getMessage());
            return -1;
        } catch (ClassNotFoundException e) {
            System.out.println("JDBC Driver not found!");
            System.out.println("You need to initialize 1st. Call initDB(...)");
            return -2;
        } catch( Exception e ) {
            System.out.println("Other exception ");
            System.out.println(e.getMessage());
            return -3;
        }
    }
    public  boolean checkConnection() {
        if (connection != null) {
            return true;
        }
        return false;
    }

    public Connection getConnection() {
    	return (connection);
    }
    public String getDriverVersion() {
    	return(strDriverVersion);
    }
    public 	int		getJDBCMajorVersion() {
    	return(intJDBCMajorVersion);
    }
    public 	int		getJDBCMinorVersion() {
    	return(intJDBCMinorVersion);
    }
    public  void closeConnection() {
        connection = null;
        databaseMetaData=null;
    }
    /*
     * 65314kkk
     */
    public  ResultSet querySQL(final String strSql) {
    	
        ResultSet 	resultSet = null;
        Statement 	statement = null;
        String		strSqlToBeUse=null;
    	
    	try {
    		statement = connection.createStatement();
    		statement.setQueryTimeout(30);  // set timeout to 30 sec.    

    		switch(strProductName.toUpperCase()) {
    			case "MARIADB":
    				/*
    				 * Same as MySQL
    				 */
    				strSqlToBeUse=strSql.replace("\\", "\\\\");
    				break;
				case "MYSQL":
					/*
					 * This doesn't work because also the * direct after the Select will be replaced :-(
					 * strSqlToBeUse=strSql.replace('*', '%');
					 * 
					 * 2016-02-03 DW: Replace \ with \\. Backslash must be masked with a \, therefore \\ with \\\\
					 */
					strSqlToBeUse=strSql.replace("\\", "\\\\");
					break;
				case "POSTGRESQL":
					/*
					 * No mask needed
					 * 
					 * 2021-07-03 DW: Also at postgres I need a mask
					 * 2025-03-26 DW: I need the mask for the bakslash
					 */
					strSqlToBeUse=strSql;
					//strSqlToBeUse=strSql.replace("\\", "\\\\");
					break;
				case "SQLITE":
					strSqlToBeUse=strSql;
					break;			
				case "TERADATA":
					strSqlToBeUse=strSql;
					break;			
				default:
					strSqlToBeUse=strSql;
    		}
    		// Excecute Query
    		strLastSQL=strSqlToBeUse;
    		resultSet = null;
    		resultSet = statement.executeQuery(strSqlToBeUse);	    
    		
    		/* 
    		 * I need for some data bases position to the 1st record. This doesn't work
    		 * in SQLite. I take the real product name from the metadata.
    		 */
    		switch(strProductName.toUpperCase()) {
    			case "MARIADB":
    				break;
    			case "MYSQL":
    				//resultSet.first();
    				//resultSet.beforeFirst();
    				break;
    			case "POSTGRESQL":
    				/*
    				 * PostgreSQL is like SQLite. It is only forward. So do nothing here.
    				 */
    				//resultSet.first();
    				//resultSet.beforeFirst();
    				break;
    			case "SQLITE":
    				break;
    			case "TERADATA":
    				break;
   			default:
    				//resultSet.first();
    				resultSet.beforeFirst();
    		}
    		/*
    		 * All fine
    		 */
    		lngResult=0;
    		return (resultSet);
    		
    	} catch(SQLException e) {
    		// if the error message is "out of memory", 
    		// it probably means no database file is found
    		/*
    		 * SQL Error
    		 */
    		strSqlErrorState=e.getSQLState();
    		strSqlErrorMessage=e.getMessage();
    		if( strSqlErrorState != null) {
        		switch(strSqlErrorState.toUpperCase()) {
    				case "42P01":
    				case "42S02":
    					/*
    					 * Table not found
    					 */
    					lngResult=-65314100;
    					return(null);
    				case "42S22":
    					/*
    					 * Unknown Columns
    					 */
    					lngResult=-65314101;
    					return(null);
    				default:
    					/*
    					 * Unknown Error
    					 */
    					lngResult=-65314002;
    					System.err.println(e.getMessage());
    					return(null);
        		}
    		} else {
    			/*
    			 * Other Error.
    			 * This could be a syntax Error
    			 */
    			lngResult=-65021199;
    			return(null);
    		} 
	    } catch(Exception e) {
	    	/*
	    	 * Other Error
	    	 */
	    	System.err.println(e.getMessage());
	    	lngResult=-65314001;
	    	return(null);
	    }
    }
    /*
     * 65021kkk
     */
    public  long executeSQL(final String strSql) {
    	
    	Statement 	statement = null;
        String		strSqlToBeUse=null;
    	
    	
    	try {
    		statement = connection.createStatement();
		    statement.setQueryTimeout(30);  // set timeout to 30 sec.
		          	
    		switch(strProductName.toUpperCase()) {
    			case "MARIADB":
    				/*
    				 * Same as MySQL
    				 */
					strSqlToBeUse=strSql.replace("\\", "\\\\");
					break;    				
				case "MYSQL":
					/*
					 * This doesn't work because also the * direct after the Select will be replaced :-(
					 * strSqlToBeUse=strSql.replace('*', '%');
					 * 
					 * 2016-02-03 DW: Replace \ with \\. Backslash must be masked with a \, therefore \\ with \\\\
					 */
					strSqlToBeUse=strSql.replace("\\", "\\\\");
					break;
				case "POSTGRESQL":
					/*
					 * No mask needed
					 */
					strSqlToBeUse=strSql;
					break;
				case "SQLITE":
					strSqlToBeUse=strSql;
					break;		
				case "TERADATA":
					strSqlToBeUse=strSql;
					break;										
				default:
					strSqlToBeUse=strSql;
					break;
    		}
    		strLastSQL	=	strSqlToBeUse;
    		lngResult	=	statement.executeUpdate(strSqlToBeUse);		  
		    return 0;
		}
    	catch(SQLException e){
    		System.out.println("ERROR executing SQL=["+strSqlToBeUse+"]");
    		// if the error message is "out of memory", 
    		// it probably means no database file is found
    		strSqlErrorState=e.getSQLState();
    		strSqlErrorMessage=e.getMessage();
    		if( strSqlErrorState != null) {
        		switch(strSqlErrorState.toUpperCase()) {
    				case "42P01":
    				case "42S02":
    					/*
    					 * 	Table not found
					 	 */
    					lngResult=-65021100;
    					return(lngResult);
    				case "42S22":
    					/*
    					 * Unknown Columns
    					 */
    					lngResult=-65021101;
    					return(lngResult);					
    				default:
    					/*
    					 * Unknown Error
    					 */
    					lngResult=-65021102;
    					System.err.println(e.getMessage());
    					return(lngResult);
        		} 
    		} else {
    			/*
    			 * Other Error.
    			 * This could be a syntax Error
    			 */
    			System.err.println(e.getMessage());				
    			lngResult=-65021199;
    			return(lngResult);
    		}   		
	    } catch(Exception e) {
	    	/*
	    	 * Other Error
	    	 */
	    	System.err.println(e.getMessage());
	    	lngResult=-65021001;
	    	return(lngResult);
	    } 	
    }
    /**
     * 65494kkk
     * 
     * @return
     */
    private String createValueStr(DataField dataField) {
    	
    	String	strValueStr		=	null;
    	try {
    		lngResult=0;
			switch(strProductName.toUpperCase()) {
				case "POSTGRESQL":
    	    		switch(dataField.intFormat ) {
    					case BOOLEAN:
    						if(dataField.bolValue == true) {
    							strValueStr="true,";
    						} else {
    							strValueStr="false,";
    						}
    						break;
    					case TEXT:
    						if ( dataField.strValue == null ) {
    							strValueStr="NULL,";
    						} else {
    							strValueStr="'"+dataField.strValue+"',";
    						}
    						break;
    					case DATE:
    						if ( dataField.datValue == null ) {
    							strValueStr="NULL,";
    						} else {
    							strValueStr="'"+ 
    									new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" ).format(dataField.datValue)+"',";
    						}
    						break;
    					case NUMBER:
    						strValueStr=dataField.douValue+",";
    						break;
    					default:
    						// type not supported
    						lngResult=-65494002;
    						return(null);
    	    		}
					break;
				default:
    	    		switch(dataField.intFormat ) {
	    				case BOOLEAN:
	    					if(dataField.bolValue == true) {
	    						strValueStr=1+",";
	    					} else {
	    						strValueStr=0+",";
	    					}
	    					break;
	    				case TEXT:
	    					if ( dataField.strValue == null ) {
	    						strValueStr="NULL,";
	    					} else {
	    						strValueStr="'"+dataField.strValue+"',";
	    					}
	    					break;
	    				case DATE:
	    					if ( dataField.datValue == null ) {
	    						strValueStr="NULL,";
	    					} else {
	    						strValueStr="'"+ 
	    								new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" ).format(dataField.datValue)+"',";
	    					}
	    					break;
	    				case NUMBER:
	    					strValueStr=dataField.douValue+",";
	    					break;
	    				default:
	    					// type not supported 
    						lngResult=-65494003;
    						return(null);
    	    		}
					break;
			}
    		/*
    		 * Done
    		 */
    		return(strValueStr);
    	} catch(Exception e){
  	      	// if the error message is "out of memory", 
  	      	// it probably means no database file is found
  	      	System.err.println(e.getMessage());
  	      	lngResult=-65494001;
  	      	return( null );
  	    }     	
    }

    /**
     * 65161kkk
     * 
     * Return:
     * 0	->	OK
     * 1	->	No Data in Array List
     * <0	->	Error 
     */
    public long insert(String strTable, ArrayList<DataField[]> dataFieldsList) {
    	
    	String 	strSql=null;
    	String	strFieldNames=null;
    	String	strValues=null;
    	String	strHelp=null;
    	int		i=0;
    	int		intFirst=0;
    	//long	lngResult=0;

    	try {
    		/*
    		 * Check for empty List
    		 */
    		if(dataFieldsList == null || dataFieldsList.size() == 0 ) {
    			return(1);
    		}
	    	strFieldNames="(";
	    	strValues="(";
    		
    		for( DataField[] dataFields : dataFieldsList ) {
    	    	if( dataFields.length==0) {
    	    		// no Data
    	    		return(intFirst);
    	    	}
    	    	if( intFirst > 0 ) {
    	    		strValues=strValues+"(";
    	    	}
    	    	for(i=0; i< dataFields.length;i++) {
	    			if(intFirst == 0) {
	    	    		switch(strProductName.toUpperCase()) {
	    					case "POSTGRESQL":
	    						/*
	    						 * PostgreSQL is w/o `` in the column name
	    						 */
	    						strFieldNames=strFieldNames+""+dataFields[i].strField+",";
	    						break;
	    					default:
	    						/*
	    						 * MariaDB, MySQL, SQLite & Terradata
	    						 * 
	    						 * The column name needs to be in quotes `...`
	    						 */
	    						strFieldNames=strFieldNames+"`"+dataFields[i].strField+"`,";
	    						break;
	    	    		}
	    	    	}
	    			/*
	    			 * Create the value String
	    			 */
	        		strHelp=createValueStr(dataFields[i]);
	        		if(strHelp==null) {
	        			/*
	        			 * Error
	        			 */
	        			return(lngResult);
	        		}
	        		strValues=strValues+strHelp;
    	    	}
    	    	/*
    			 * The last character is now a comma --> delete and add )
    			 */
    			strValues = strValues.substring(0, strValues.length()-1)+"),\n";    			
    			intFirst++;
    		}
    		strFieldNames = strFieldNames.substring(0, strFieldNames.length()-1)+")";
    		strValues = strValues.substring(0, strValues.length()-2)+";";    			
			
			switch(strProductName.toUpperCase()) {
				case "POSTGRESQL":
					/*
					 * PostgreSQL is w/o `` in the caolumn name
					 */
					strSql="INSERT INTO "+strTable+" "+strFieldNames+" VALUES "+strValues;
					break;
				default:
					/*
					 * 	MariaDB, MySQL, SQLite & Teradata
				 	 * 
				 	 * The columnname needs to be in quotes `...`
				 	 */
					strSql="INSERT INTO `"+strTable+"` "+strFieldNames+" VALUES "+strValues;
					break;
			}    		
    		/*
    		 * Execute the SQL
    		 */
    		lngResult=executeSQL(strSql);
    		/*
    		 * Return from the function
    		 */
    		if (lngResult != 0)  {
    			return(-65013004);
    		} else {
    			return(lngResult);
    		}
    	} catch(Exception e){
  	      	// if the error message is "out of memory", 
  	      	// it probably means no database file is found
  	      	System.err.println(e.getMessage());
  	      	return( -65161001);
  	    }  
    }

    /**
     * 65013kkk
     * 
     * Return:
     * 0	->	OK
     * 1	->	No Data in dataFields
     * <0	->	Error 
     */
    public long insert(String strTable, DataField dataFields[]) {
    	
    	ArrayList<DataField[]> 		dataFieldsList 			= 	new ArrayList<DataField[]>();
    	
    	dataFieldsList.clear();
    	dataFieldsList.add(dataFields);
    	
    	lngResult = insert(strTable, dataFieldsList );
    	
    	return(lngResult);
    }
    /*
     * 65014kkk
     */
    public long update(String strTable, DataField dataFields[], DataField whereFields[]) {
    	String 	strSql="";
    	int		i=0;
    	//long	lngResult=0;
    	
    	/*
    	 * is Data in the Area ?
    	 */
    	if( dataFields.length==0) {
    		// no Data
    		return(1);
    	}
    	/*
    	 * If the whereFields are empty I do nothing 
    	 */
    	if( whereFields.length==0) {
    		// no Data
    		return(-65014001);
    	}    	
    	/*
    	 * Build the SQL String
    	 *
    	 * Example:
    	 * 	UPDATE table_name
		 *	SET column1=value1,column2=value2,...
		 *	WHERE some_column=some_value;
    	 */	
    	for(i=0; i< dataFields.length;i++) {
    		switch(dataFields[i].intFormat ) {
    			case TEXT:
    				switch(strProductName.toUpperCase()) {
    					case "POSTGRESQL":
    						/*
    						 * PostgreSQL is w/o `` in the column name
    						 */
    						if ( dataFields[i].strValue == null ) {
    							strSql=strSql+""+dataFields[i].strField+"=NULL,";
    						} else {
    							strSql=strSql+""+dataFields[i].strField+"='"+dataFields[i].strValue+"',";
    						}
    						break;
    					default:
    						/*
    						 * MariaDB, MySQL and SQLite
    						 * 
    						 * The column name needs to be in quotes `...`
    						 */
    						if ( dataFields[i].strValue == null ) {
    							strSql=strSql+"`"+dataFields[i].strField+"`=NULL,";
    						} else {
    							strSql=strSql+"`"+dataFields[i].strField+"`='"+dataFields[i].strValue+"',";
    						}
    						break;
    				}
    				break;
    			case DATE:
        			switch(strProductName.toUpperCase()) {
    					case "POSTGRESQL":
    						/*
    						 * PostgreSQL is w/o `` in the column name
    						 */
    						if ( dataFields[i].datValue == null ) {
    	    					strSql=strSql+""+dataFields[i].strField+"=NULL,";
    	    				} else {
    	    					strSql=strSql+""+dataFields[i].strField+"='"+
    	    						new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" ).format(dataFields[i].datValue)+"',";
    	    				}						
    						break;
    					default:
    						/*
    						 * MariaDB, MySQL and SQLite
    						 * 
    						 * The column name needs to be in quotes `...`
    						 */
    						if ( dataFields[i].datValue == null ) {
    	    					strSql=strSql+"`"+dataFields[i].strField+"`=NULL,";
    	    				} else {
    	    					strSql=strSql+"`"+dataFields[i].strField+"`='"+
    	    						new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" ).format(dataFields[i].datValue)+"',";
    	    				}						
    						break;
    				}
    				break;
    			case NUMBER:
        			switch(strProductName.toUpperCase()) {
    					case "POSTGRESQL":
    						/*
    						 * PostgreSQL is w/o `` in the column name
    						 */
    	    				strSql=strSql+""+dataFields[i].strField+"="+dataFields[i].douValue+",";
    	    				break;
    					default:
    						/*
    						 * MariaDB, MySQL and SQLite
    						 * 
    						 * The column name needs to be in quotes `...`
    						 */
    	    				strSql=strSql+"`"+dataFields[i].strField+"`="+dataFields[i].douValue+",";
    						break;
    				}
    				break;
    			case BOOLEAN:
        			switch(strProductName.toUpperCase()) {
    					case "POSTGRESQL":
    						/*
    						 * PostgreSQL is w/o `` in the column name
    						 */
    	    				if(dataFields[i].bolValue=true) {
    	    					strSql=strSql+""+dataFields[i].strField+"= true,";
    	    				} else {
    	    					strSql=strSql+""+dataFields[i].strField+"= false,";
    	    				}
    	    				break;
    					default:
    						/*
    						 * MariaDB, MySQL and SQLite
    						 * 
    						 * The column name needs to be in quotes `...`
    						 */
    	    				if(dataFields[i].bolValue=true) {
    	    					strSql=strSql+"`"+dataFields[i].strField+"`="+1+",";
    	    				} else {
    	    					strSql=strSql+"`"+dataFields[i].strField+"`="+0+",";
    	    				}
    						break;
    				}
    				break;
    				
    			default:
    				// type not supported 
    				return(-65014002);
    		}
    	}
    	// cut the last comma
    	strSql = strSql.substring(0, strSql.length()-1);
    	/*
    	 * Now the Where Piece --> this must come. We have guaranteed a WHERE Data. I checked it at the start of the function
    	 */
    	strSql=strSql+createWhereSQLStatement(whereFields);
    	
    	/*
    	 * Finalize the SQL Statement
    	 */
		switch(strProductName.toUpperCase()) {
			case "POSTGRESQL":
				/*
				 * 	PostgreSQL is w/o `` in the column name
				 */
				strSql = "UPDATE "+strTable+" SET " + strSql;
				break;
			default:
				/*
				 * MariaDB, MySQL and SQLite
				 * 
				 * The column name needs to be in quotes `...`
				 */
				strSql = "UPDATE `"+strTable+"` SET " + strSql;
				break;
		}
    	/*
		 * Execute the SQL
		 */
		lngResult=executeSQL(strSql);
		
		/*
		 * Return from the function
		 */
		if (lngResult != 0)  {
			return(-65014004);
		} else {
			return(lngResult);
		}
    }
    /*
     * 65015kkk
     */
    public long delete(String strTable,  DataField whereFields[]) {
    	String 	strSql=null;
    	//long	lngResult=0;
    	
    	/*
    	 * If the whereFields are empty I do nothing 
    	 */
    	if( whereFields.length==0) {
    		// no Data
    		return(-65015001);
    	}    	
    	/*
    	 * Build the SQL String
    	 *
    	 * Example:
    	 * 	DELETE table_name
		 *	WHERE some_column=some_value;
		 *
		 * The DELETE Table will be added at the end
    	 */	
    	/*
    	 * Now the Where Piece --> this must come
    	 * 
    	 */
    	strSql=createWhereSQLStatement(whereFields);
    	/*
    	 * Final SQL Statement
    	 */
		switch(strProductName.toUpperCase()) {
			case "POSTGRESQL":
				/*
				 * 	PostgreSQL is w/o `` in the column name
				 */
		    	strSql = "DELETE FROM "+strTable+" " + strSql;
				break;
			default:
				/*
				 * MariaDB, MySQL, SQLite & Teradata
				 * 
				 * The columnname needs to be in quotes `...`
				 */
		    	strSql = "DELETE FROM `"+strTable+"` " + strSql;
				break;
		}
    	/*
		 * Execute the SQL
		 */
		lngResult=executeSQL(strSql);
		/*
		 * Return from the function
		 */
		if (lngResult != 0)  {
			return(-65015003);
		} else {
			return(lngResult);
		}
    }
    
    /*
     * 61001nnn
     */
    /**
     * 61001nnn
     * 
     * Read a configuration valaue from T__Configuration
     * 
     * @param strConfigurationSection
     * @param strConfigurationParameter
     * @return Value of the configuration parameter
     * 
	 * lngResult:
	 * 	  0	--> ok
	 *    1	--> Parameter not found
	 *    2	--> Parameter more than one time in the table
	 *  < 0	--> Error
	 * 
	 */
	public String getConfigurationValue( String strConfigurationSection, String strConfigurationParameter ) {
		String strConfigValue="";
		String strSql;
		int intRsCount=0;
		
		//Open the connection to the configuration DB
	    try
	    {
	    	// Build the SQL String
		    // strSql = "SELECT strValue "+
		    strSql = "SELECT * "+
		            " FROM "+ ConstantDB.CONFIGURATION_TABLE  +  
		            " WHERE (((strConfigurationSection)= '" + strConfigurationSection + "')" + 
		            " AND" +
		            " ((strConfigurationParameter)= '" + strConfigurationParameter + "')) ";
			
		    // Execute Query
		    ResultSet rs = querySQL(strSql);	

		    /*
		     * Check for Error
		     */
		    if( rs == null) {
		    	lngResult = -61001003;
		    	return(null);
		    }
		    if (rs.isAfterLast() ) {
		    	// Parameter or section not found
		    	// Implement also a Message box to be shown later
		    	//Db.closeConnection();
		    	
		    	storeMsg(61001001, "Can't find Configuration Parameter [" +
		    				strConfigurationParameter +
		    				"] in Section [" + strConfigurationSection + "]", "");
		    	lngResult=1;
		    	return("");
		    } else {
		    	intRsCount=0;
		    	while (rs.next()){
		    		strConfigValue = rs.getString("strValue");
		    		intRsCount++;
		    	}
		    	switch(intRsCount) {
		    		case 0:
		    			storeMsg(61001001, "Can not find Configuration Parameter [" +
			    				strConfigurationParameter +
			    				"] in Section [" + strConfigurationSection + "]", "");
		    			lngResult=1;
		    			return("");		    			
		    		case 1:
		    			storeMsg(61001001, "Configuration Parameter [" +
			    				strConfigurationParameter +
			    				"] in Section [" + strConfigurationSection + "] read", "[" + strConfigValue + "]");
		    			lngResult=0;
		    			return(strConfigValue);
		    		default:
		    			storeMsg(61001002, "Configuration Parameter [" +
			    				strConfigurationParameter +
			    				"] in Section [" + strConfigurationSection + "] found more than one time ", String.valueOf(intRsCount));
		    			lngResult=2;
		    			return("");		    			
		    	}
		    }		    	
	    } catch(Exception e){
	    	// if the error message is "out of memory", 
	    	// it probably means no database file is found	    	
	    	System.err.println(e.getMessage());
	    	lngResult=-61001001;
	    	return(null);
	    }
	}
	/*
	 * 61003nnn
	 */	
	public long storeMsg( long lngCode, String strMsgText1, String strMsgText2 ) {
		
		int					intTabFieldLngID=	-1;
		long 				lngNextID		=	-1;
		Date 				timeStamp 		= 	new Date();
		String 				strDate 		= 	ConstantDB.DATEFORMAT.format(timeStamp);
		String 				strTime 		= 	ConstantDB.TIMEFORMAT.format(timeStamp);
		String 				strSql 			= 	"";
	
		if (intFirst==1) {
			intFirst=0;
			try {
				java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
				strHostname= localMachine.getHostName();				
			} catch(Exception e) {
				System.out.println(e.getClass());
			}
			/*
			 * Get the log TableFields
			 */
			TableFields logTableFields 	= getTableFields(ConstantDB.LOG_TABLE);
			switch(strProductName.toUpperCase()) {
				case "POSTGRESQL":
					intTabFieldLngID 		= logTableFields.findTableColumn("lngid");
					break;
				default:
					intTabFieldLngID 		= logTableFields.findTableColumn("lngID");
					break;
			}					
			bolIsLogLngIDSerial=logTableFields.getAutoIncrement(intTabFieldLngID);
		}
		if (lngCode < ConstantDB.DEBUGLEVEL.getDebugLevel()){
		    
			// Mask ' and "" in Msg1 and Msg2
			if (strMsgText1 != null ) {
				strMsgText1=strMsgText1.replaceAll("'", "''");
				strMsgText1=strMsgText1.replaceAll("\"", "\"\"");
			}
			if (strMsgText2 != null ) {
				strMsgText2=strMsgText2.replaceAll("'", "''");
				strMsgText2=strMsgText2.replaceAll("\"", "\"\"");
			}
			if(bolIsLogLngIDSerial) {
				/*
				 * Auto Increment
				 */
				strSql="insert into " + ConstantDB.LOG_TABLE + " (strLogDate,strLogTime,strHostname,strUsername,strProcessname,lngLogCode,strLogMessage1, strLogMessage2) " +
	    		  		"values(" + "'" +
	    		  		strDate + "','" +
	    		  		strTime + "','" +
	    		  		strHostname + "','" +
	    		  		strUsername + "','" +
	    		  		strProcessname + "'," +
	    		  		lngCode + ",'" +
	    		  		strMsgText1 + "','" +
	    		  		strMsgText2 +"')";
			} else {
				/*
				 * No Auto Increment
				 */
				/*
				 * Get Next lngID
				 */
				try {
					strSql="SELECT MAX(lngID) AS lngMaxID FROM "+ConstantDB.LOG_TABLE;		
					ResultSet rs = querySQL(strSql);
					/*
					 * check for Error
					 */
					if( rs==null) {
						System.out.println("ERROR ["+strSqlErrorState+"9] -->"+strSqlErrorMessage);
						return(-6103002);
					}
					while (rs.next()) {
						lngNextID=rs.getLong("lngMaxID")+1;
					}
					rs.close();				
				} catch (Exception e) {
		        	System.err.println(e.getMessage());	        	
		        	return(-6103001);
		    	}
				// create the SQL Statement --> could be changed to DataField technology later. I don't know what this helps
				strSql="insert into " + ConstantDB.LOG_TABLE + " (lngID, strLogDate,strLogTime,strHostname,strUsername,strProcessname,lngLogCode,strLogMessage1, strLogMessage2) " +
			    		  		"values(" +
			    		  		lngNextID +", '" +
			    		  		strDate + "','" +
			    		  		strTime + "','" +
			    		  		strHostname + "','" +
			    		  		strUsername + "','" +
			    		  		strProcessname + "'," +
			    		  		lngCode + ",'" +
			    		  		strMsgText1 + "','" +
			    		  		strMsgText2 +"')";
			}
			/*
			 * Execute the SQl Statement
			 */
			lngResult=executeSQL(strSql);
			
			if ( bolShowErrorMessageBox && lngCode < 0  ) {
				// show message box
				//showErrorDialog( "Error :" + lngCode +"\n\n" + strMsgText1 + "\n" + strMsgText2 );
			}
			if ( bolPrintErrorMessageBox && lngCode < 0  ) {
				System.err.println("ERROR "+lngCode+" at "+ strDate +" "+strTime+" : " + strMsgText1+"; "+strMsgText2);
			}
		   	return( lngResult);		      
		} else {
			return 0;
		}
	}
	public void setShowErrorMessageBox( boolean bolFlag ) {
		bolShowErrorMessageBox=bolFlag;
	}
	public boolean getShowErrorMessageBox( ) {
		return(bolShowErrorMessageBox);
	}
	public void setPrintErrorMessageBox( boolean bolFlag ) {
		bolPrintErrorMessageBox=bolFlag;
	}
	public boolean getPrintErrorMessageBox( ) {
		return(bolPrintErrorMessageBox);
	}
	/* 
	 * 61004kkk
	 * 
	 * Value
	 * "" --> not found
	 * null --> error
	 */
	/**
	 * 61004kkk
	 * 
	 * Read a parameter from the Parameter table (default T__Parameter
	 * @param strParameter to get the value from
	 * @return value of the parameter
	 * 
	 * lngResult:
	 * 	  0	--> ok
	 *    1	--> Parameter not found
	 *    2	--> Parameter more than one time in the table
	 *  < 0	--> Error
	 * 
	 */
	public String getParameter( String strParameter ) {
		
		String 	strSql		=	null;
		String	strValue	=	null;
		int		intRsCount		=	0;
		
		try {
			strSql = 	"SELECT strValue " + " FROM "+ ConstantDB.PARAMETER_TABLE  +
						" WHERE (((strParameter)= '" + strParameter + "')) ";
			ResultSet rs = querySQL(strSql);

		    /*
		     * Check for Error
		     */
		    if( rs == null) {
		    	lngResult=-61004002;
		    	return(null);
		    }
			if (rs.isAfterLast() ) {
		    	// Parameter or section not found
		    	// Implement also a Message box to be shown later
		    	//Db.closeConnection();
		    	lngResult = 1;
		    	storeMsg(61004002, "Can't find Parameter [" + strParameter + "] in Table [" + ConstantDB.PARAMETER_TABLE + "]", "");
		    	return("");
			} else {
				intRsCount=0;
		    	while (rs.next()){
		    		strValue = rs.getString("strValue");
		    		intRsCount++;
		    	}
		    	switch(intRsCount) {
		    		case 0:
		    			storeMsg(61004002, "Can not find Parameter [" + strParameter + "] in Table [" + ConstantDB.PARAMETER_TABLE + "]", "");
		    			lngResult=1;
		    			return("");		    			
		    		case 1:
		    			storeMsg(61004001, "Read Parameter [" + strParameter + "] from Table [" + ConstantDB.PARAMETER_TABLE + "]", strValue);
		    			lngResult=0;
			    		return(strValue);
		    		default:
		    			storeMsg(61004003, "Parameter [" + strParameter + "] too many times in Table [" + ConstantDB.PARAMETER_TABLE + "]", String.valueOf(intRsCount));
		    			lngResult=2;
		    			return("");		    			
		    	}				
			}
		} catch(Exception e) {
			System.err.println(e.getMessage());
			lngResult=-61004099;
			return(null);
	    }
	}
	/* 
	 * 61005kkk
	 */
	public long setParameter(String strParameter, String strValue) {
		long 		lngResult	=	0;
		String		strHelp		=	null;
		/*
		 * First I need to check if Parameter is already in the table. If so, I will update, otherwise insert
		 */
		strHelp=getParameter(strParameter);
		if(this.lngResult==0) {
			/*
			 * Parameter is in the table --> update
			 */
			lngResult=update(	ConstantDB.PARAMETER_TABLE, 
					new DataField [] {
							/*
							 * Fields to change
							 */
							new DataField("strValue",strValue)
					},
					new DataField [] {
							/* 
							 * 	WHERE Statement
							 */
							new DataField("strParameter", strParameter)
					}
				);
			return(lngResult);
		} else {
			/*
			 * I need to insert
			 */
			lngResult=insert(	ConstantDB.PARAMETER_TABLE, 
					new DataField [] {
							/*
							 * Fields to change
							 */
							new DataField("strParameter", strParameter),
							new DataField("strValue",strValue)
					}
				);
			return(lngResult);
		}
	}
    /*
     * 65075kkk
     */
    public	String	createWhereSQLStatement(DataField whereFields[]) {
    	int			i			=	0;
    	String		strSql		=	null;
    	
    	/*
    	 * EmptyList ?
    	 */
    	if(whereFields.length == 0 ) {
    		return("");
    	} else {
    		strSql=" WHERE (";
        	
        	for(i=0; i< whereFields.length;i++) {
        		switch(whereFields[i].intFormat ) {
        			case TEXT:
            			switch(strProductName.toUpperCase()) {
        					case "POSTGRESQL":
        						/*
        						 * PostgreSQL is w/o `` in the column name
        						 * 2019-03-31 DW: The first IF could not happen, but I don't know what happens if I delete it.It should produce an error
        						 */
        	    				if ( whereFields[i].strValue == null ) {
        	    					strSql=strSql+"("+whereFields[i].strField+"=NULL) AND ";
        	    				} else if ( whereFields[i].strValue.toUpperCase().equals("IS NULL")) {
        	    					strSql=strSql+"("+whereFields[i].strField+" IS NULL) AND ";
        	    				} else {
        	    					if( whereFields[i].strValue.indexOf('%')>0) {
        	    						strSql=strSql+"("+whereFields[i].strField+" LIKE '"+whereFields[i].strValue+"') AND ";
        	    					} else {
        	    						strSql=strSql+"("+whereFields[i].strField+"='"+whereFields[i].strValue+"') AND ";
        	    					}
        	    				}
        	    				break;
        					default:
        						/*
        						 * MariaDB, MySQL,SQLite & Teradata
        						 * 
        						 * The column name needs to be in quotes `...`
        						 */
        	    				if ( whereFields[i].strValue == null ) {
        	    					strSql=strSql+"(`"+whereFields[i].strField+"`=NULL) AND ";
        	    				} else if ( whereFields[i].strValue.toUpperCase().equals("IS NULL")) {
        	    					strSql=strSql+"("+whereFields[i].strField+" IS NULL) AND ";
        	    				} else {
        	    					if( whereFields[i].strValue.indexOf('%')>0) {
        	    						strSql=strSql+"(`"+whereFields[i].strField+"` LIKE '"+whereFields[i].strValue+"') AND ";
        	    					} else {
        	    						strSql=strSql+"(`"+whereFields[i].strField+"`='"+whereFields[i].strValue+"') AND ";
        	    					}
        	    				}
        						break;
        				}
        				break;
        			case DATE:
            			switch(strProductName.toUpperCase()) {
        					case "POSTGRESQL":
        						/*
        						 * PostgreSQL is w/o `` in the column name
        						 */
        	    				if ( whereFields[i].datValue == null ) {
        	    					strSql=strSql+"("+whereFields[i].strField+"=NULL) AND ";
        	    				} else {
        	    					strSql=strSql+""+whereFields[i].strField+"(='"+
        	    						new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" ).format(whereFields[i].datValue)+"') AND ";
        	    				}
        	    				break;
        					default:
        						/*
        						 *  MariaDB, MySQL,SQLite & Teradata
        						 * 
        						 * The column name needs to be in quotes `...`
        						 */
        	    				if ( whereFields[i].datValue == null ) {
        	    					strSql=strSql+"(`"+whereFields[i].strField+"`=NULL) AND ";
        	    				} else {
        	    					strSql=strSql+"`"+whereFields[i].strField+"(`='"+
        	    						new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" ).format(whereFields[i].datValue)+"') AND ";
        	    				}
        						break;
        				}
        				break;
        			case NUMBER:
            			switch(strProductName.toUpperCase()) {
        					case "POSTGRESQL":
        						/*
        						 * PostgreSQL is w/o `` in the column name
        						 */
        	    				strSql=strSql+"("+whereFields[i].strField+"="+whereFields[i].douValue+") AND ";
        	    				break;
        					default:
        						/*
        						 *  MariaDB, MySQL,SQLite & Teradata
        						 * 
        						 * The column name needs to be in quotes `...`
        						 */
        	    				strSql=strSql+"(`"+whereFields[i].strField+"`="+whereFields[i].douValue+") AND ";
        						break;
        				}
        				break;
        			case BOOLEAN:
            			switch(strProductName.toUpperCase()) {
        					case "POSTGRESQL":
        						/*
        						 * PostgreSQL is w/o `` in the column name
        						 */
        	    				if(whereFields[i].bolValue=true) {
        	    					strSql=strSql+"("+whereFields[i].strField+"="+1+") AND ";
        	    				} else {
        	    					strSql=strSql+"("+whereFields[i].strField+"="+0+") AND ";
        	    				}
        	    				break;
        					default:
        						/*
        						 *  MariaDB, MySQL,SQLite & Teradata
        						 * 
        						 * The column name needs to be in quotes `...`
        						 */
        						if(whereFields[i].bolValue=true) {
        							strSql=strSql+"(`"+whereFields[i].strField+"`="+1+") AND ";
        	    				} else {
        	    					strSql=strSql+"(`"+whereFields[i].strField+"`="+0+") AND ";
        	    				}        						
        	    				break;
        				}
        				break;
        			default:
        				// type not supported 
        				return(null);
        		}
        	}
        	/* 
        	 * Cut off the AND and add )	
        	 */
        	strSql=strSql.substring(0, strSql.length()-5)+")";
        	return(strSql);
    	}
    }
    /*
     * 61151kkk
     */
    public	String	concateFieldFromTable( String strTableName, String strFieldName, String	strDelimiter, DataField whereFields[]) {
    	
    	int				intFirst		=	1;
    	String			strResult		=	"";
    	String			strSql			=	null;
    	ResultSet		rs				=	null;
    	
    	try {
    		strSql="SELECT * FROM '"+strTableName+"'";
    		/*
    		 * Is there a where statement
    		 */
    		strSql=strSql+createWhereSQLStatement(whereFields);
    		rs = querySQL(strSql);
    		if( fieldExist(rs, strFieldName) > 0 ) {
    			while( rs.next() ) {
    				if(intFirst==1) {
    					intFirst=0;
    					strResult=rs.getString(strFieldName);
    				} else {
    					strResult=strResult+strDelimiter+rs.getString(strFieldName);
    				}
    			}
    			if(intFirst==1) {
    				/*
    				 * Table is empty
    				 */
    				storeMsg(-61151003, "WARNING concateFieldFromTable: Table is empty whith the where statement", strSql);
    			}
    			return(strResult);
    		} else {
    			storeMsg(-61151002, "ERROR concateFieldFromTable: Field does not exist", strFieldName);
    			return(null);
    		}
    	} catch(Exception e) {
			System.err.println(e.getMessage());
			return(null);
	    }
    }
    /*
     * 61035kkk
     * 
     */
    public	long	fieldExist( ResultSet rs, String strFieldName) {
    	
    	int				i			=	0;
    	String			strHelp		=	null;
    	
    	try {
    		for(i=0 ; i<rs.getMetaData().getColumnCount(); i++){
                strHelp=rs.getMetaData().getColumnName(i+1);
                if(strFieldName.equals(strHelp)) {
                	/*
                	 * Field found
                	 */
                	return(1);
                }
            }
    		/*
    		 * Field not found
    		 */
    		return(0);
    	} catch(Exception e) {
    		System.err.println(e.getMessage());
			return(-61035001);
    	}
    }
    public long	printDataFields(DataField dataFields[]) {
    	int	i=0;
    	for( i=0;i<dataFields.length;i++) {
    		switch(dataFields[i].intFormat ) {
				case BOOLEAN:
					System.out.println(" "+i+": [" +dataFields[i].intFormat+"] Field=["+dataFields[i].strField+"] Value=["+dataFields[i].bolValue+"]");
					break;
				case DATE:
					SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");    	
					System.out.println(" "+i+": [" +dataFields[i].intFormat+"] Field=["+dataFields[i].strField+"] Value=["+simpleDateFormat.format(dataFields[i].datValue)+"]");
					break;
				case NUMBER:
					System.out.println(" "+i+": [" +dataFields[i].intFormat+"] Field=["+dataFields[i].strField+"] Value=["+dataFields[i].douValue+"]");
					break;
				case TEXT:
					System.out.println(" "+i+": [" +dataFields[i].intFormat+"] Field=["+dataFields[i].strField+"] Value=["+dataFields[i].strValue+"]");					
					break;
				default:
					break;
    		}
    	}
    	return(0);
    }
    /*
     * 65082kkk
     */
    public long printTableInfo(String strTableName ) {
    	int						i					=	0;
    	int						j					=	0;
    	ResultSet				rs					=	null;
    	ResultSet				rs1					=	null;
    	ResultSetMetaData 		rsmd 				= 	null;
    	ResultSetMetaData 		rsmd1 				= 	null;
    	String					strSql				=	" SELECT * FROM "+strTableName+"";
    	
    	try {
    		rs = querySQL(strSql);
    		if(rs==null) {
    			return(-65082002);
    		}
    		rsmd=rs.getMetaData();
    		System.out.println("\nColumns [" +rsmd.getColumnCount()+"]");
			
    		for(i=1 ; i<=rsmd.getColumnCount(); i++){
    			System.out.println("\nColumn " +i);
    			System.out.println("---------------------");
    			System.out.println("Column name 		["+rsmd.getColumnName(i)+"]");
    			System.out.println("Column type 		["+rsmd.getColumnType(i)+"]");
    			System.out.println("Column type name 	["+rsmd.getColumnTypeName(i)+"]");
    			System.out.println("Column display size	["+rsmd.getColumnDisplaySize(i)+"]");
    			System.out.println("Column precision  	["+rsmd.getPrecision(i)+"]");
    			System.out.println("Column scale  		["+rsmd.getScale(i)+"]");
    			System.out.println("Column table  		["+rsmd.getTableName(i)+"]");
    			System.out.println("Column schema  		["+rsmd.getSchemaName(i)+"]");
    			System.out.println("Column catalog name	["+rsmd.getCatalogName(i)+"]");
    			
    			rs1 = databaseMetaData.getColumns(connection.getCatalog(), databaseMetaData.getUserName(),strTableName,rsmd.getColumnName(i));
    			rsmd1=rs1.getMetaData();
    			while (rs1.next()) {
            		System.out.println("---------------------");
        			for(j=1 ; j<=rsmd1.getColumnCount(); j++){
            			System.out.println(rsmd1.getColumnName(j)+"   ["+rs1.getString(j)+"]");
            		}
        			System.out.println("---------------------");
    			}
    		}
    		rs.close();
    		System.out.println("---------------------");
    		System.out.println("Primary Keys ");
    		System.out.println("---------------------");
    		rs = databaseMetaData.getPrimaryKeys(null, null, strTableName);
    		rsmd=rs.getMetaData();
    		System.out.println("\nPrimary Keys [" +rsmd.getColumnCount()+"]");
			
			while (rs.next()) {
    			System.out.println("---------------------");
    			for(i=1 ; i<=rsmd.getColumnCount(); i++){
        			System.out.println(rsmd.getColumnName(i)+"   ["+rs.getString(i)+"]");
        		}
			}
			
    		return(0);	
    	} catch (Exception e) {
    		System.err.println(e.getMessage());
			return(-65082001);
		} 
    }
    /*
     * 65311kkk
     */
    public long dropTable( String strTable ) {
    	
    	long			lngResult				=	0;
    	String			strSql					=	null;
    	try {
    		
			switch(strProductName.toUpperCase()) {
				case "POSTGRESQL":
					/*
					 * PostgreSQL is w/o `` in the Table name
					 */
					strSql="DROP TABLE IF EXISTS "+strTable+";";
					break;
				default:
					/*
					 * 	MariaDB, MySQL, SQLite & Teradata
					 * 
					 * 	The Table name needs to be in quotes `...`
					 */
					strSql="DROP TABLE IF EXISTS `"+strTable+"`;";
				break;
			}    		
			/*
			 * Execute the SQL
			 */
			lngResult=executeSQL(strSql);
    		return(lngResult);
    		
    	} catch(Exception e) {
    		System.err.println(e.getMessage());
			return(-65311001);
    	}
    }
    /*
     * 65312kkk
     */

    public long createTable( String strTable, TableField tableFields[] ) {
    	
    	boolean			bolExitLoop				=	false;
    	int				intPrimaryKeyCount		=	0;
    	int				intFieldCount			=	0;
    	long			lngResult				=	0;
    	String			strSql					=	null;
    	String			strPrimaryKey			=	null;
    	String			strHelp					=	null;
    	try {
    		/*
    		 * start the SQL Comment
    		 */
			switch(strProductName.toUpperCase()) {
				case "POSTGRESQL":
					/*
					 * PostgreSQL is w/o `` in the Table name
					 */
					strSql="CREATE TABLE "+strTable+" (\n";
					break;
				default:
					/*
					 * 	MariaDB, MySQL, SQLite & Teradata
					 * 
					 * 	The Table name needs to be in quotes `...`
					 */
					strSql="CREATE TABLE `"+strTable+"` (";
					break;
			}     		
    		/*
    		 * Loop over all Fields
    		 */
    		for( TableField tableField:tableFields) {
    			strHelp = tableField.createCmd(strProductName.toUpperCase());
    			if(intFieldCount >0) {
    				strSql=strSql+ ",\n     "+strHelp;
    			} else {
    				/* First Field
    				 */
    				strSql=strSql+ "\n     "+strHelp;
    			}
    			intFieldCount++;
    		}
    		/*
    		 * Primary Key
    		 */
    		strPrimaryKey="PRIMARY KEY( ";
    		intPrimaryKeyCount=0;
    		for( TableField tableField:tableFields) {
    			if( tableField.getPrimaryKeyFlag()) {
    				
    				switch(strProductName.toUpperCase()) {
    					case "POSTGRESQL":
    						/*
    						 * PostgreSQL is w/o `` in the Table name
    						 */
    						if( intPrimaryKeyCount>0) {
    							strPrimaryKey=strPrimaryKey + ", " + tableField.getFieldName();
    						} else {
    							strPrimaryKey=strPrimaryKey + tableField.getFieldName();
    						}
    						break;
    					case "SQLITE":
    						/*
    						 * SQLite braucht eine Sonderbehandlung für den Primary Key bei SERIAL Types.
    						 */
    						if(tableField.getCreateFormat()==TableFieldCreateFormat.SERIAL) {
    							/*
    							 * No additional PRIMARY KEY LINE allowed
    							 */
    							intPrimaryKeyCount=0;
    							bolExitLoop=true;
    						}
    						if( intPrimaryKeyCount>0) {
    							strPrimaryKey=strPrimaryKey + ", `" + tableField.getFieldName()+"`";
    						} else {
    							strPrimaryKey=strPrimaryKey + "`" + tableField.getFieldName()+"`";
    						}
    						break;
    					default:
    						/*
    						 * 	MariaDB, MySQL, SQLite & Teradata
    						 * 
    						 * 	The Table name needs to be in quotes `...`
    						 */
    						if( intPrimaryKeyCount>0) {
    							strPrimaryKey=strPrimaryKey + ", `" + tableField.getFieldName()+"`";
    						} else {
    							strPrimaryKey=strPrimaryKey + "`" + tableField.getFieldName()+"`";
    						}
    						break;
    				} 
    				if(bolExitLoop) {
    					break;
    				}
    				intPrimaryKeyCount++;
    			}
    		}
    		if( intPrimaryKeyCount >0 ) {
    			/*
    			 * Add Primary Key to the strSql
    			 */
    			strSql	=	strSql + ",\n" + strPrimaryKey +")";
    		} else {
    			/*
    			 * No Primary Key --> do nothing
    			 */
    		}
    		/*
    		 * Finish the SQL Command
    		 */
    		strSql=strSql+");";
        	/*
    		 * Execute the SQL
    		 */
    		lngResult=executeSQL(strSql);
    		

    		return(lngResult);
    	} catch(Exception e) {
    		System.err.println(e.getMessage());
			return(-65312001);
    	}
    }
    /*
     * 65313kkk
     */
    public long dropView( String strView ) {
    	
    	long			lngResult				=	0;
    	String			strSql					=	null;
    	try {
    		
			switch(strProductName.toUpperCase()) {
				case "POSTGRESQL":
					/*
					 * PostgreSQL is w/o `` in the Table name
					 */	
					strSql="DROP VIEW IF EXISTS "+strView+";";
					break;
				default:
					/*
					 * 	MariaDB, MySQL, SQLite & Teradata
					 * 
					 * The Table name needs to be in quotes `...`
					 */
					strSql="DROP VIEW IF EXISTS `"+strView+"`;";
					break;
			}    		
			/*
			 * Execute the SQL
			 */
			lngResult=executeSQL(strSql);
			
    		return(lngResult);
    	} catch(Exception e) {
    		System.err.println(e.getMessage());
			return(-65313001);
    	}
    }   
    /*
     * 65319kkk
     */
    public long executeSqlScript( String strFilename ) {
    	
    	String					strLine				=	null;
    	
    	try {
       		/*
       		 * Check if Script exists
       		 */
			File targetDB= new File(strFilename);
			if (!targetDB.exists()) {
				/*
				 * Script does not exists
				 */
				return(-65319002);
			}
			/*
			 * Read the script
			 * 
			 * I have some options
			 * a) read all and then execute
			 * b) check the commands, once I got a ";" --> I execute
			 */
			BufferedReader reader = new BufferedReader(new FileReader(strFilename));
			StringBuilder scriptContent = new StringBuilder();
			while ((strLine = reader.readLine()) != null) {
                scriptContent.append(strLine).append("\n");
            }
			reader.close();
       		/*
       		 * Done
       		 */
       		return(0);
       		
    	} catch(Exception e) {
    		System.err.println(e.getMessage());
			return(-65319001);
    	}
    }
    
    /*
     * 65354kkk
     */
    public	TableFields getTableFields( String strTable ) {
    	
		
       	try {

			TableFields sourceTableFields = new TableFields(this, strTable);
			return(sourceTableFields);
			
		} catch (Exception e) {
        	System.err.println(e.getMessage());
        	lngResult=-65354001;
        	return(null);
        }
    }
    /*
     * 65355kkk
     */
    public TableFieldCreateFormat getCreateFormat( String strDataType) {
    	
    	TableFieldCreateFormat enFormat	=	TableFieldCreateFormat.TEXT;
    	try {
    		switch( strDataType.toUpperCase()) {
    			case "BLOB":
    			case "LONG":            		
    				return(TableFieldCreateFormat.BLOB);
    			case "BIGINT":
        		case "INT8":
        			return(TableFieldCreateFormat.BIGINT);
        		case "INT":
        		case "INT4":
        		case "INTEGER":
        		case "INT2":            		
        		case "SMALLINT":
        		case "TINYINT":
        			return(TableFieldCreateFormat.INTEGER);
        		case "BIT":
        		case "BOOL":
        		case "BOOLEAN":
        			return(TableFieldCreateFormat.BOOLEAN);
        		case "DATE":
            	case "DATETIME":	                		
            	case "TIME":
            	case "TIMESTAMP":
            		return(TableFieldCreateFormat.DATE);
            	case "DECIMAL":
            	case "DOUBLE":
            	case "FLOAT":
            	case "FLOAT8":
            	case "REAL":
            		return(TableFieldCreateFormat.DOUBLE);
            	case "SERIAL":
            		return(TableFieldCreateFormat.SERIAL);            		
            	case "TEXT":
            		return(TableFieldCreateFormat.TEXT);
            	case "VARCHAR":
            		return(TableFieldCreateFormat.VARCHAR);
    			default:
    				return null;
    		}   		
		} catch (Exception e) {
        	System.err.println(e.getMessage());
        	lngResult=-65354001;
        	return(null);
        }
    }
}