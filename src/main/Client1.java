import java.io.*;
import java.net.*;
import java.util.*;

public class Client1{
    public static void main(String[] args) throws Exception
    {
        Scanner input = new Scanner(System.in);
        String serverResponse;

        System.out.println("Enter the name of the file to request");
        String fileInput = input.nextLine();

        Socket socket = new Socket("localhost",8000);

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println(fileInput);
        serverResponse = in.readLine();
        if(serverResponse != null) System.out.println("The file contents are: " + serverResponse);
        else System.out.println("The requested file does not exist");
    }
}
