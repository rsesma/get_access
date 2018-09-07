package com.leam.stata.getaccess;

import java.sql.*;

import com.stata.sfi.Data;
import com.stata.sfi.SFIToolkit;

public class GetDataFromAccess {
	
    public enum StataType {
        NUMBER, STRING, DATE, BOOLEAN
    }

	public static int getData(String args[]) {
		String db = args[0];
		String table = args[1];
		int rc = 0;
		
		try {
			Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");						// loading Driver			
			Connection conn = DriverManager.getConnection("jdbc:ucanaccess://" + db);		// establish connection
			Statement s = conn.createStatement();
			ResultSet rs = s.executeQuery("SELECT * FROM [" + table + "]");				// get all data from table
			
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();			// the column count starts from 1
			// create variables
			for (int i = 1; i <= columnCount; i++ ) {
				String name = rsmd.getColumnName(i);
				StataType type = toStataType(rsmd.getColumnTypeName(i));
				
				switch (type) {
					case NUMBER:
					case BOOLEAN:
						rc = Data.addVarDouble(name);
						break;
					case STRING:
						rc = Data.addVarStr(name,2045);
						break;
					case DATE:
						rc = Data.addVarStr("_Date_" + name,2045);
						break;
				}
				if (rc!=0) {
					SFIToolkit.errorln("error adding variables");
					return(rc);
				}
			}
			
			// add data
			long obs = 0;
			while (rs.next()) {
                obs++;
                rc = Data.setObsTotal(obs);
                if (rc!=0) {
                	SFIToolkit.errorln("error inserting rows");
                	return(rc);
                }
                
				for (int i = 1; i <= columnCount; i++ ) {
					StataType type = toStataType(rsmd.getColumnTypeName(i));
                    
					switch (type) {
						case NUMBER:
						case BOOLEAN:
							double v = rs.getDouble(i);
							if (!rs.wasNull()) rc = Data.storeNum(i, obs, v);
							break;
						case STRING:
						case DATE:
							String c = rs.getString(i);
							if (!rs.wasNull()) rc = Data.storeStr(i, obs, c);
							break;
					}
                	if(rc!=0) {
                		SFIToolkit.errorln("error inserting rows");
                		return(rc);
                	}
				}
			}
						
		} catch (Exception e) {
			SFIToolkit.errorln("error connecting to database (" + e.getMessage() + ")");
			return (198);
		}
		
		return (rc);
	}
	
	public static StataType toStataType(String type) {

		switch (type) {
            case "CHAR":
            case "VARCHAR":
            case "LONGVARCHAR":
                return (StataType.STRING);

            case "NUMERIC":
            case "DECIMAL":
            case "TINYINT":
            case "SMALLINT":
            case "INTEGER":
            case "BIGINT":
            case "REAL":
            case "FLOAT":
            case "DOUBLE":
            case "BINARY":
            case "VARBINARY":
            case "LONGVARBINARY":
            	return (StataType.NUMBER);

            case "DATE":
            case "TIME":
            case "TIMESTAMP":
            	return (StataType.DATE);
            	
            case "BIT":
                return (StataType.BOOLEAN);
        }

        return (null);
    }

}
