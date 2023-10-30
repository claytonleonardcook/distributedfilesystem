package com.cs.distributedfilesystem;

import java.io.*;
import java.net.*;
import java.util.*;

public class Client1{
    static Scanner input = new Scanner(System.in);
    public static void main(String[] args) throws Exception
    {
        System.out.println("Enter 1 to retrive a file, 2 save to save a new file: ");
        int action = input.nextInt();
        if(action == 1) getFile();
        else postFile();
    }

    public static void getFile() throws IOException {
        String serverResponse;
        System.out.println("Enter the name of the file to request: ");
        String fileName = input.nextLine();

        Socket socket = new Socket("localhost",8000);

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println(fileName);
        serverResponse = in.readLine();
        if(serverResponse != null) System.out.println("The file contents are: " + serverResponse);
        else System.out.println("The requested file does not exist");
    }

    public static void postFile() throws IOException {
        String serverResponse;
        System.out.println("Enter the name of the file to save: ");
        String fileName = input.nextLine();
        System.out.println("Enter the file contents: ");
        String fileContents = input.next();

        Socket socket = new Socket("localhost",8000);

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println(fileName);
        out.println(fileContents);
        serverResponse = in.readLine();
        if(serverResponse.equals("200")) System.out.println("The file " + fileName + " has been successfully saved.");
    }

}
