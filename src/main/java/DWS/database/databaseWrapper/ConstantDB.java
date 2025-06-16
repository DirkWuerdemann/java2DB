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

import java.text.SimpleDateFormat;
import DWS.utilities.fileUtilities.FileUtilities;

public interface ConstantDB {
	
	/*
	 * Version of the wrapper
	 */
	public static final String VERSION = "0.0.4";
	
	/*
	 *  Debug Level
	 *  Default is only errors will be locked
	 */
	public static final DebugLevel DEBUGLEVEL = new DebugLevel(0); 

	/*
	 * Date & Time format
	 */
	public static final SimpleDateFormat DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd");
	public static final SimpleDateFormat TIMEFORMAT = new SimpleDateFormat("HH:mm:ss.SSS");
	
	/*
	 * File Function(s)
	 */
	public static final FileUtilities FILE = new FileUtilities();
			
	/*
	 * Default Tables
	 * 
	 * 2025-04-07 DW: Changed to lower case
	 */
	//public static final String	CONFIGURATION_TABLE = "T__Configuration";
	//public static final String	LOG_TABLE 			= "T__Log";
	//public static final String	PARAMETER_TABLE 	= "T__Parameter";
	public static final String	CONFIGURATION_TABLE = "t__configuration";
	public static final String	LOG_TABLE 			= "t__log";
	public static final String	PARAMETER_TABLE 	= "t__parameter";
	/*
	 * Default VARCHAR Size
	 */
	public static final int		VARCHAR_SIZE		=	255;
}
