package example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;

class RequestClass {
    String endPoint;
    String user;
    String password;
    String databaseName;
    String databaseType;

    public String getendPoint() {
        return endPoint;
    }
    public void setendPoint(String endPoint) {
        this.endPoint = endPoint;
    }
    public String getuser() {
        return user;
    }
    public void setuser(String user) {
        this.user = user;
    }
    public String getpassword() {
        return password;
    }
    public void setpassword(String password) {
        this.password = password;
    }
    public String getdatabaseName() {
        return databaseName;
    }
    public void setdatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
    public String getdatabaseType() {
        return databaseType;
    }
    public void setdatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }
    public RequestClass(String endPoint, String user, String password, String databaseName, String databaseType) {
        this.endPoint = endPoint;
        this.user = user;
        this.password = password;
        this.databaseName = databaseName;
        this.databaseType = databaseType;
    }
    public RequestClass() {}
    
}

class ResponseClass {
    Boolean result;
    String message;
    List<String> tables;

    public Boolean getresult() {
        return this.result;
    }
    public void setresult(Boolean result) {
        this.result = result;
    }
    public String getmessage() {
        return this.message;
    }
    public void setmessage(String message) {
        this.message = message;
    }
    public List<String> gettables() {
        return this.tables;
    }
    public void settables(List<String> tables) {
        this.tables = tables;
    }
    public ResponseClass(Boolean result, String message, List<String> tables) {
        this.result = result;
        this.message = message;
        this.tables = tables;
    }
    public ResponseClass(Boolean result, String message) {
        this.result = result;
        this.message = message;
    }    
    public ResponseClass() {}
}

public class JDBCConnect implements RequestHandler<RequestClass, ResponseClass>{
    private List<String> queryDB(String jdbcEndPoint, String databaseName, String user, String password, String databaseType) throws Exception {
        try {
            Connection conn = DriverManager.getConnection(String.format("%s%s", jdbcEndPoint, databaseName), user, password);
            Statement statement = conn.createStatement();
            ResultSet result = statement.executeQuery(String.format("Select table_name from INFORMATION_SCHEMA.TABLES where `table_schema` = '%s'", databaseName));
            int columnCount = result.getMetaData().getColumnCount();
            List<String> tables = new ArrayList<String>();
            System.out.println("------Tables------");
            while(result.next()) {
                tables.add(result.getString(1));
                for (int i = 1; i <= columnCount; i += 1 ) {
                    System.out.println(String.format("%s \t", result.getString(i)));
                }
            }
            System.out.println("------------------");
            conn.close();
            return tables;
        } catch (Exception error) {
            throw error;
        }
    }

    public ResponseClass handleRequest(RequestClass body, Context context) {
        try {
            System.out.printf("[EndPoint]: %s\n", body.endPoint);
            System.out.printf("[DatabaseName]: %s\n", body.databaseName);
            System.out.printf("[DatabaseType]: %s\n", body.databaseType);
            System.out.printf("[User]: %s\n", body.user);
            System.out.printf("[Password]: %s\n", body.password);
            Boolean paramsCheckResult = Stream
                .of(new String[]{ body.endPoint, body.databaseName, body.user, body.password, body.databaseType})
                .filter(params -> params == null || params.equals(""))
                .count() > 0;
            if (paramsCheckResult) {
                System.out.printf("[ERROR]: %s\n", "Missing params");
                return new ResponseClass(false, "Missing params");
            }
            switch (body.databaseType) {
                case "mysql":{
                    Class.forName("com.mysql.cj.jdbc.Driver");
                    break;
                }
                default: {
                    System.out.printf("[ERROR]: %s\n", "Unknown database type");
                    return new ResponseClass(false, "Unknown database type"); 
                }
            }
            List<String> tables = queryDB(body.endPoint, body.databaseName, body.user, body.password, body.databaseType);
            return new ResponseClass(true, "Succeed.", tables);
        } catch(ClassNotFoundException error) {
            System.out.printf("[ERROR]: %s\n", error.toString());
            return new ResponseClass(false, "Driver not found");
        } catch (CommunicationsException error) {
            System.out.printf("[ERROR]: %s\n", error.toString());
            return new ResponseClass(false, "Connection fail");
        } catch (SQLSyntaxErrorException error) {
            System.out.printf("[ERROR]: %s\n", error.toString());
            return new ResponseClass(false, "Unknown database");
        } catch (SQLException error) {
            System.out.printf("[ERROR]: %s\n", error.toString());
            return new ResponseClass(false, "Verify fail");
        } catch(Exception error) {
            System.out.printf("[ERROR]: %s\n", error.toString());
            return new ResponseClass(false, error.toString());
        }
    }
}