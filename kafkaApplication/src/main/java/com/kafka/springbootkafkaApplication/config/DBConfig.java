package com.kafka.springbootkafkaApplication.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Configuration
public class DBConfig {
    @Bean
    public Connection openConnection() throws SQLException {
        //load the diver
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException cnfe) {
            System.out.println("Error loading driver: " + cnfe);
            cnfe.printStackTrace();
        }

        //define the connection URL
        String host = "127.0.0.1";
        String dbName = "SUBSCRIPTION_SYSTEM";
        int port = 3306;
        String oracleURL = "jdbc:mysql://" + host + ":" + port + "/" + dbName;

        String username = "root";
        String password = "admin";

        System.out.println("Connected to DB.");

        return DriverManager.getConnection(oracleURL, username, password);
    }
}
