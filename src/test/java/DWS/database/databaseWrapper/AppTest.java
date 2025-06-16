package DWS.database.databaseWrapper;


//import DWS.database.databaseWrapper.Db;

public class AppTest {

    public static void main( String[] args )
    {
    	
        System.out.println( "Test start ..." );
        
        /*
         * Create a new Database
         */
        Db dbtest = new Db();
        
        System.out.println("Database Wrapper Version:   "+dbtest.getVersion());
        System.out.println("Database Debug Level:       "+ConstantDB.DEBUGLEVEL.getDebugLevel());
        
        System.out.println( "Test done ;-)" );
    }
}
