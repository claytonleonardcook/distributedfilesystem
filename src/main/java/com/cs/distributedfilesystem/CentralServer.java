package com.cs.distributedfilesystem;

import java.io.*;
import java.net.*;
import java.sql.*;

public class CentralServer {
    public static void main(String[] args) throws Exception {
        int portNumber = 8000;
        String clientInput, sndMessage;

        ServerSocket serversocket = new ServerSocket(portNumber);

        System.out.println("Waiting for incoming connections");
        Socket socket = serversocket.accept();

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        clientInput = in.readLine();

        String url = "jdbc:sqlite:fileLocations.db";
        Connection connection = DriverManager.getConnection(url);
        PreparedStatement ps = connection.prepareStatement("SELECT locations FROM fileLocations WHERE name = ?");
        ps.setString(1, clientInput);
        ResultSet rs = ps.executeQuery();//get the result from the database

        System.out.println("database response:" + rs.getString(1));

        if(rs.getString(1) != null)
            sndMessage = rs.getString(1);
        else sndMessage = "404";//404 error: file not found

        out.println(sndMessage);
    }
}
