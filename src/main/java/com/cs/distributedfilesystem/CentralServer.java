package com.cs.distributedfilesystem;

import org.springframework.web.bind.annotation.GetMapping;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;

public class CentralServer {
    static String url = "jdbc:sqlite:fileLocations.db";
    static String sndMessage;
    static Connection connection;

    static {
        try {
            connection = DriverManager.getConnection(url);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static String LS1 = " ";
    static String LS2 = " ";
    public static void main(String[] args) throws Exception {
        int portNumber = 8000;
        String clientInput;

        ServerSocket serversocket = new ServerSocket(portNumber);

        System.out.println("Waiting for incoming connections");
        Socket socket = serversocket.accept();

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        clientInput = in.readLine();

        //if get
        getFile(out, in);
        //if post
//        postFile(out, in);


    }

    public static void getFile(PrintWriter out, BufferedReader in) throws SQLException, IOException {

        String fileName = in.readLine();
        PreparedStatement ps = connection.prepareStatement("SELECT locations FROM fileLocations WHERE name = ?");
        ps.setString(1, fileName);
        ResultSet rs = ps.executeQuery();//get the result from the database

        System.out.println("database response:" + rs.getString(1));

        if(rs.getString(1) != null)
            sndMessage = rs.getString(1);
        else sndMessage = "404";//404 error: file not found

        out.println(sndMessage);
    }

    public static void postFile(PrintWriter out, BufferedReader in) throws SQLException, IOException {
        String url = "jdbc:sqlite:fileLocations.db";
        String fileName = in.readLine();
        String fileContents = in.readLine();

        String[] fileArr = fileContents.split(" ");
        ArrayList<String> locationArr = new ArrayList<String>();
        String locationStr = "";

        String curLocation = LS1;
        for(int i = 0; i < fileArr.length; i+=5){
            locationArr.add(curLocation);
            if(curLocation.equals(LS1)) curLocation = LS2;
            else curLocation = LS1;
        }

        for(int i =0; i < locationArr.size(); i++) locationStr += locationArr.get(i);


        Connection connection = DriverManager.getConnection(url);
        PreparedStatement ps = connection.prepareStatement("INSERT INTO fileLocations VALUES(?, ?, ?)");
        ps.setString(1, fileName);
        ps.setString(2, String.valueOf(locationArr.size()));
        ps.setString();
        ResultSet rs = ps.executeQuery();//get the result from the database

        System.out.println("database response:" + rs.getString(1));

        if(rs.getString(1) != null)
            sndMessage = rs.getString(1);
        else sndMessage = "404";//404 error: file not found

        out.println(sndMessage);
    }

    @GetMapping("/ping")
    public int ping() {
        return 200;
    }

}
