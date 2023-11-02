import java.io.*;
import java.net.*;
import java.util.*;

public class Client extends DistributedFileSystem {
    private Runnable leafBroadcast;

    public Client() throws Exception {
        this.leafBroadcast = new Runnable() {
            public void run() {
                while (true) {
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
                            Thread.sleep(1000);
                        }
                    } catch (Exception e) {
                        System.err.println(e);
                    }
                }
            }
        };

        if (ENV.equals("DEV")) {
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
     * Asks the user for the file name and retrieves the file. If the file is found,
     * the file contents are displayed.
     * Else, and error message is displayed.
     * 
     * @throws IOException
     */
    public void getFile() {
        Scanner input = new Scanner(System.in);

        System.out.println("Enter the name of the file to request: ");
        String fileName = input.nextLine();

        try {
            if (ENV.equals("DEV")) {
                synchronized (leafServers) {
                    while (leafServers.isEmpty())
                        ;
                }
            }

            int random = new Random().nextInt(leafServers.size());
            Object randomKey = leafServers.keySet().toArray()[random];
            IPAddress leaf = leafServers.get(randomKey);

            System.out.println(leaf);

            Socket socket = new Socket(leaf.address, leaf.port);
            SocketIO inout = new SocketIO(socket);

            DistributedFileSystem.sendGetFile(inout, fileName);
            String serverResponse = inout.readLine();

            if (!serverResponse.equals("500") && !serverResponse.equals(NOTFOUND))
                System.out.println("The file contents are: " + serverResponse);
            else
                System.out.println("The file " + fileName + " does not exist");

            socket.close();
        } catch (Exception e) {
            System.err.println(e);
        }

        input.close();
    }

    /**
     * Asks the use to enter the file name and file contents and sends a success
     * message to the user once the file
     * has been successfully saved
     * 
     * @throws IOException
     */
    public void postFile() {
        Scanner input = new Scanner(System.in);

        System.out.println("Enter the name of the file to save: ");
        String fileName = input.nextLine();
        System.out.println("Enter the file contents: ");
        String fileContents = input.nextLine();

        try {
            if (ENV.equals("DEV")) {
                synchronized (leafServers) {
                    while (leafServers.isEmpty())
                        ;
                }
            }

            int random = new Random().nextInt(leafServers.size());
            Object randomKey = leafServers.keySet().toArray()[random];
            IPAddress leaf = leafServers.get(randomKey);

            System.out.println(leaf);

            Socket socket = new Socket(leaf.address, leaf.port);
            SocketIO inout = new SocketIO(socket);

            sendPostFile(inout, fileName, fileContents);
            String serverResponse = inout.readLine();
            if (serverResponse.equals("200"))
                System.out.println("The file " + fileName + " has been successfully saved.");

            socket.close();
        } catch (Exception e) {
            System.err.println(e);
        }

        input.close();
    }

    public void talkToLeaf() {
        final int RETREIVEFILE = 1,
                        SAVEFILE = 2;
        Scanner input = new Scanner(System.in);
        while (true) {
            try {
                System.out.println("Enter 1 to retrieve a file, 2 save to save a new file: ");
                int action = input.nextInt();

                switch (action) {
                    case RETREIVEFILE:
                        getFile();
                        break;
                    case SAVEFILE:
                        postFile();
                        break;
                    default:
                        System.err.println("Incorrect input! Try again.");
                        break;
                }
            } catch (Exception e) {
                System.err.println(e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Client client = new Client();
    }
}