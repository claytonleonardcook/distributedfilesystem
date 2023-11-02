import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

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
     * @param out
     * @param in
     */
    public void getFile(SocketIO inout) {

        try {
            Connection connection = DriverManager.getConnection(url);
            String fileName = inout.readLine();
            PreparedStatement ps = connection.prepareStatement("SELECT locations FROM fileLocations WHERE name = ?");
            ps.setString(1, fileName);
            ResultSet rs = ps.executeQuery();// get the result from the database

            System.out.println("database response:" + rs.getString(1));

            if (rs.getString(1) == null) {
                inout.println(NOTFOUND);
                throw new Exception("File not found!");
            }

            // DistributedFileSystem.sendGetLocations(in, out, sndMessage);
            inout.println(rs.getString(1));
        } catch (Exception e) {
            System.err.println(e);
        }

    }

    /**
     * Central Server posts a new file to the database, and sends a success code
     * back to the leaf
     * 
     * @param out
     * @param in
     */
    public void postFile(SocketIO inout) {

        try {
            String fileName = inout.readLine();
            String fileContents = inout.readLine();

            System.out.println("File name: " + fileName + "/n File Contents: " + fileContents);

            // int fileContentsSize = fileContents.split(",").length;

            Connection connection = DriverManager.getConnection(url);
            PreparedStatement ps = connection.prepareStatement("INSERT INTO fileLocations VALUES(?, ?)");
            // ps.setString(1, "");
            ps.setString(1, fileName);
            // ps.setString(2, String.valueOf(fileContentsSize));
            ps.setString(2, fileContents);
            ps.executeUpdate();

            // PreparedStatement ps2 = connection.prepareStatement("SELECT locations FROM
            // fileLocations WHERE name = ?");
            // ps2.setString(1, fileName);
            // ResultSet rs = ps2.executeQuery();//get the result from the database
            //
            // System.out.println("database added the following locations:" +
            // rs.getString(1));
            // DistributedFileSystem.sendPostLocations(in, out, "200");
            inout.println(OK);
        } catch (Exception e) {
            System.err.println(e);
        }

    }

    private void router(SocketIO inout, String method, String endpoint) {
        if (method.contains("GET")) {
            switch (endpoint) {
                case "/file":
                    this.getFile(inout);
                    break;
                default:
                    inout.println(BADREQUEST);
            }
        } else if (method.contains("POST")) {
            switch (endpoint) {
                case "/file":
                    this.postFile(inout);
                    break;
                default:
                    inout.println(BADREQUEST);
            }
        } else {
            inout.println(BADREQUEST);
        }
    }

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