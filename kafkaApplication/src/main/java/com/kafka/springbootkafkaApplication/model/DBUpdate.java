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

    public String update(String topicName, String subscriber) throws SQLException, IOException {

        //check whether table is exists
        DatabaseMetaData dbm = conn.getMetaData();

        // check if 'subscription' table is there
        ResultSet tables = dbm.getTables(null, null, "subscription", null);
        if (!tables.next()) {
            // Table does not exist, create the table
            ScriptRunner runner=new ScriptRunner(conn);
            InputStreamReader reader = new InputStreamReader(new FileInputStream("foo.sql"));
            runner.runScript(reader);
            reader.close();
            System.out.println("---------Table does not exist.");

        }
        System.out.println("Updating data...");
        System.out.println("Inserting: (" + topicName +", "+subscriber + ").");

        //update table file
        PreparedStatement insertStatement = conn.prepareStatement("INSERT INTO subscription VALUES(?, ?)");
        insertStatement.setString(1, topicName);
        insertStatement.setString(2, subscriber);
        insertStatement.executeUpdate();
        insertStatement.close();

        return "Update success";
    }




}
