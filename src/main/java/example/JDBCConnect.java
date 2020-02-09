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
    public RequestClass(String endPoint, String user, String password, String databaseName) {
        this.endPoint = endPoint;
        this.user = user;
        this.password = password;
        this.databaseName = databaseName;
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
    private List<String> queryDB(String jdbcEndPoint, String databaseName, String user, String password) throws Exception {
        try {
            Connection conn = DriverManager.getConnection(String.format("%s%s", jdbcEndPoint, databaseName), user, password);
            Statement statement = conn.createStatement();
            ResultSet result = statement.executeQuery(String.format("Select table_name from INFORMATION_SCHEMA.TABLES where `table_schema` = '%s'", databaseName));
            int columnCount = result.getMetaData().getColumnCount();
            List<String> tables = new ArrayList<String>();
            while(result.next()) {
                tables.add(result.getString(1));
                for (int i = 1; i <= columnCount; i += 1 ) {
                    System.out.print(String.format("%s \t", result.getString(i)));
                }
                System.out.println();
            }
            conn.close();
            return tables;
        } catch (Exception error) {
            System.out.println(error.toString());
            throw error;
        }
    }

    public ResponseClass handleRequest(RequestClass body, Context context) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            System.out.printf("[endPoint]: %s\n", body.endPoint);
            System.out.printf("[databaseName]: %s\n", body.databaseName);
            System.out.printf("[user]: %s\n", body.user);
            System.out.printf("[password]: %s\n", body.password);
            Boolean paramsCheckResult = Stream
                .of(new String[]{ body.endPoint, body.databaseName, body.user, body.password})
                .filter(params -> params == null || params.equals(""))
                .count() > 0;
            if (paramsCheckResult) {
                return new ResponseClass(false, "Missing params");
            }
            List<String> tables = queryDB(body.endPoint, body.databaseName, body.user, body.password);
            return new ResponseClass(true, "Succeed.", tables);
        } catch(ClassNotFoundException error) {
            System.out.print("Driver not found");
            return new ResponseClass(false, "Driver not found");
        } catch (CommunicationsException error) {
            System.out.print(error.toString());
            return new ResponseClass(false, "Connection fail");
        } catch (SQLSyntaxErrorException error) {
            System.out.print(error.toString());
            return new ResponseClass(false, "Unknown database");
        } catch (SQLException error) {
            System.out.print(error.toString());
            return new ResponseClass(false, "Verify fail");
        } catch(Exception error) {
            System.out.print(error.toString());
            return new ResponseClass(false, error.toString());
        }
    }
}