import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Central server that hosts a SQLite server to keep track of which leafs are
 * hosting which file segments
 * 
 * @author Clayton Cook
 * @author Siona Beaudoin
 */
public class Central extends DistributedFileSystem {
    final static String url = "jdbc:sqlite:fileLocations.db";

    private ServerSocket central;
    private Runnable leafBroadcast;

    public Central() throws Exception {
        this(4000);
    }

    public Central(int port) throws Exception {
        this.central = new ServerSocket(port);

        this.broadcast = new Runnable() {
            public void run() {
                try {
                    DatagramSocket centralBroadcast = new DatagramSocket();
                    centralBroadcast.setBroadcast(true);

                    byte[] buf = new byte[1];
                    DatagramPacket packet = new DatagramPacket(buf, buf.length,
                            InetAddress.getByName("255.255.255.255"), port + 1);

                    System.out.println(String.format("I'm the central server and I'm here @ %s!",
                            InetAddress.getLocalHost()));
                    while (true) {
                        centralBroadcast.send(packet);
                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    System.err.println(e);
                }

            }
        };

        this.leafBroadcast = new Runnable() {
            public void run() {
                try {
                    DatagramSocket leafBroadcast = new DatagramSocket();
                    leafBroadcast.setBroadcast(true);
                    byte[] buf = new byte[1];
                    DatagramPacket packet = new DatagramPacket(buf, buf.length,
                            InetAddress.getByName("255.255.255.255"), port + 1);

                    while (true) {
                        System.out.println("Waiting for data");
                        leafBroadcast.receive(packet);
                        System.out.println("Data received");
                        System.out.println(packet.getData());
                    }
                } catch (Exception e) {
                    System.err.println(e);
                }
            }

        };

        if (ENV.equals("DEV")) {
            Thread broadcastThread = new Thread(this.broadcast);
            broadcastThread.start();
            Thread leafBroadcastThread = new Thread(this.leafBroadcast);
            leafBroadcastThread.start();
            Thread leafThread = new Thread(() -> this.talkToLeaf());
            leafThread.start();
        } else {
            Thread leafThread = new Thread(() -> this.talkToLeaf());
            leafThread.start();
        }
    }

    /**
     * Central Server retrieves file locations from the database and send them to
     * the leaf server
     * 
     * @param inout -> Socket the central is using to communicate to a leaf
     */
    public void getFile(SocketIO inout) {

        try {
            // connect to the database
            Connection connection = DriverManager
                    .getConnection("jdbc:sqlite:C:/Users/clcook1/Documents/distributedfilesystem/fileLocations.db");
            String fileName = inout.readLine();// get the file name from a leaf
            // get the location string corresponding to the file name from the database
            PreparedStatement ps = connection.prepareStatement("SELECT locations FROM fileLocations WHERE name = ?");
            ps.setString(1, fileName);
            ResultSet rs = ps.executeQuery();// get the result from the database

            System.out.println("database response:" + rs.getString(1));

            if (rs.getString(1) == null) {// if the file name is not in the database, send an error code
                inout.println(NOTFOUND);
                throw new Exception("File not found!");
            }

            inout.println(rs.getString(1));// send the file contents back to the leaf
        } catch (Exception e) {
            System.err.println(e);
        }

    }

    /**
     * Central Server posts a new file to the database, and sends a success or error
     * code back to the leaf
     * 
     * @param inout -> Socket the central is using to communicate to a leaf
     */
    public void postFile(SocketIO inout) {

        try {
            String fileName = inout.readLine();// get the file name
            String fileContents = inout.readLine();// get the file locations string

            System.out.println("File name: " + fileName + "/n File Contents: " + fileContents);
            // connect to the database
            Connection connection = DriverManager
                    .getConnection("jdbc:sqlite:C:/Users/clcook1/Documents/distributedfilesystem/fileLocations.db");
            // add the file name and locations to the database
            PreparedStatement ps = connection.prepareStatement("INSERT INTO fileLocations VALUES(?, ?)");
            ps.setString(1, fileName);
            ps.setString(2, fileContents);
            ps.executeUpdate();

            inout.println(OK);
        } catch (Exception e) {
            System.err.println(e);
        }

    }

    /**
     *
     * @param inout
     * @param method
     * @param endpoint
     */
    private void router(SocketIO inout, String method, String endpoint) {
        System.out.println(method);
        if (method.contains("GET")) {
            switch (endpoint) {
                case "/locations":
                    this.getFile(inout);
                    break;
                default:
                    inout.println(BADREQUEST);
            }
        } else if (method.contains("POST")) {
            switch (endpoint) {
                case "/locations":
                    this.postFile(inout);
                    break;
                default:
                    inout.println(BADREQUEST);
            }
        } else {
            inout.println(BADREQUEST);
        }
    }

    /**
     * Method to find a leaf
     */
    private void talkToLeaf() {
        while (true) {
            try {
                System.out.println("Waiting for incoming connections");
                Socket leaf = central.accept();
                SocketIO inout = new SocketIO(leaf);

                String[] request = inout.readLine().split(" ");
                if (request.length != 2) {
                    inout.println(BADREQUEST);
                    continue;
                }
                String method = request[0];
                String endpoint = request[1];

                router(inout, method, endpoint);

                inout.close();
                leaf.close();
            } catch (Exception e) {
                System.err.println(e);
            }
        }
    }

    // private void broadcast() {
    // while (true) {
    // try {
    // DatagramSocket centralBroadcast = new DatagramSocket(4000,
    // InetAddress.getByName("0.0.0.0"));
    // centralBroadcast.setBroadcast(true);
    // byte[] buf = new byte[32];
    // DatagramPacket packet = new DatagramPacket(buf, buf.length);

    // while (true) {
    // centralBroadcast.send(packet);
    // }
    // } catch (Exception e) {
    // System.err.println(e);
    // }
    // }
    // }

    public static void main(String[] args) throws Exception {
        Central central = new Central();
    }
}