package com.kafka.springbootkafkaApplication.model;

import java.io.*;
import java.sql.*;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DBUpdate {
    @Autowired
    private Connection conn;

    public String insert(String topicName, String subscriber) throws SQLException, IOException {

        DatabaseMetaData dbm = conn.getMetaData();

        // check if 'subscription' table is there
        ResultSet tables = dbm.getTables(null, null, "subscription", null);
        if (!tables.next()) {
            System.out.println("---------Table does not exist. Creating table...");

            //if table does not exist, create the table
            ScriptRunner runner=new ScriptRunner(conn);
            InputStreamReader reader = new InputStreamReader(new FileInputStream("src/main/resources/database/createTable.sql"));
            runner.runScript(reader);
            reader.close();
        }
        System.out.println("Updating data...");
        System.out.println("Inserting: (" + topicName +", "+subscriber + ").");

        //update table file
        PreparedStatement insertStatement = conn.prepareStatement("INSERT INTO subscription VALUES(?, ?)");
        insertStatement.setString(1, topicName);
        insertStatement.setString(2, subscriber);
        insertStatement.executeUpdate();
        insertStatement.close();

        System.out.println("Update success.");
        return "Update success";
    }

    public String delete(String topicName, String subscriber) throws IOException {
        String deleteStr = "DELETE FROM subscription WHERE topicName = '" + topicName + "' AND subscriber='"+subscriber+"';";
        System.out.println("Deleting subscription...");
        System.out.println(deleteStr);

        InputStream deleteSQL = new ByteArrayInputStream(deleteStr.getBytes());
        ScriptRunner runner=new ScriptRunner(conn);
        InputStreamReader reader = new InputStreamReader(deleteSQL);
        runner.runScript(reader);
        reader.close();

        return "Delete subscription: subscriber - " + subscriber + ", topic - " + topicName;
    }




}
