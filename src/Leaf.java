import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.Scanner;

/**
 * Leaf server that hosts segments of files and compiles files if contacted by a
 * client
 * 
 * @author Clayton Cook
 * @author Siona Beaudoin
 */
public class Leaf extends DistributedFileSystem {
    /** Socket that server is hosted on */
    private ServerSocket leaf;
    private Runnable centralBroadcast;

    /**
     * Initialize class attributes, create required directories, start server on
     * 3000 and start thread to listen for client
     * 
     * @throws Exception
     */
    public Leaf() throws Exception {
        this(3000);
    }

    /**
     * Initialize class attributes, create required directories, start server on
     * {@param port} and start thread to listen for client
     * 
     * @param port Port to host leaf on
     * @throws Exception
     */
    public Leaf(int port) throws Exception {
        File segmentsFile = new File("./segments");
        segmentsFile.mkdirs();

        this.leaf = new ServerSocket(port);

        this.broadcast = new Runnable() {
            public void run() {
                try {
                    DatagramSocket leafBroadcast = new DatagramSocket();
                    leafBroadcast.setBroadcast(true);

                    byte[] buf = new byte[1];
                    DatagramPacket packet = new DatagramPacket(buf, buf.length,
                            InetAddress.getByName("255.255.255.255"), port + 1);

                    System.out.println(String.format("I'm a leaf and I'm here @ %s!", InetAddress.getLocalHost()));
                    while (true) {
                        leafBroadcast.send(packet);
                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    System.err.println(e);
                }
            }

        };

        this.centralBroadcast = new Runnable() {
            public void run() {
                try {
                    DatagramSocket centralBroadcast = new DatagramSocket();
                    byte[] buf = new byte[1];
                    DatagramPacket packet = new DatagramPacket(buf, buf.length,
                            InetAddress.getByName("255.255.255.255"), port + 1);

                    while (true) {
                        System.out.println("Waiting for data");
                        centralBroadcast.receive(packet);
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
            Thread centralBroadcastThread = new Thread(this.centralBroadcast);
            centralBroadcastThread.start();
            Thread clientThread = new Thread(() -> this.talkToClient());
            clientThread.start();
        } else {
            Thread clientThread = new Thread(() -> this.talkToClient());
            clientThread.start();
        }
    }

    /**
     * Leaf server receives a GET request for a file
     * 
     * @param inout Input & output for socket
     */
    private void getFile(SocketIO inout) {
        try {
            String name = inout.readLine();

            System.out.println(String.format("GET file(%s)", name));

            if (ENV.equals("DEV")) {
                synchronized (centralServer) {
                    while (centralServer == null)
                        ;
                }
            }

            // Establish connection with central
            Socket central = new Socket(centralServer.address, centralServer.port);
            SocketIO centralInout = new SocketIO(central);

            String allHostsAndSegments = sendGetLocations(centralInout, name);

            System.out.printf("Got \"%s\" from central server!\n", allHostsAndSegments);

            // If the central server returned a interal server error then close the
            // connection and throw an exception
            if (allHostsAndSegments.equals("500")) {
                centralInout.close();
                central.close();
                throw new Exception("Central server couldn't find segments for file!");
            }

            // Split up all pairs of hosts and segment sequence numbers
            String[] hostsAndSegments = allHostsAndSegments.split(",");

            // String where file segments will be concatenated to and eventually sent to
            // client
            String file = "";

            for (String hostAndSegment : hostsAndSegments) {
                String host = hostAndSegment.split(":")[0];
                int segment = Integer.parseInt(hostAndSegment.split(":")[1]);

                // If the host isn't apart of the global leaf server hash map then close
                // connections and throw exception
                if (leafServers.get(host) == null) {
                    centralInout.close();
                    central.close();
                    throw new Exception("Couldn't find leaf!");
                }

                // Segment of file that's retreived by current leaf or another leaf
                String fileSegment = "";

                // If host is the current machine then get file from directory
                if (host.equals(IP)) {
                    fileSegment = readFile(String.format("./segments/%s/%d.txt", name, segment));
                    System.out.printf("Got \"%s\" from myself\n", fileSegment);
                } else {
                    System.out.printf("Requesting segment, %d, from %s\n", segment, host);

                    int port = leafServers.get(host).port;

                    // Establish connection with leaf with file segment
                    Socket leaf = new Socket(host, port);
                    SocketIO leafInout = new SocketIO(leaf);

                    fileSegment = sendGetSegment(leafInout, name, segment);

                    // If the leaf server sent back a internal server error then close connections
                    // and throw exception
                    if (fileSegment.equals(INTERNALSERVERERROR)) {
                        leafInout.close();
                        leaf.close();
                        throw new Exception(
                                String.format("Leaf %s either didn't have or couldn't find segment %d", host, segment));
                    }

                    System.out.printf("Got \"%s\" from %s\n", fileSegment, host);

                    leafInout.close();
                    leaf.close();
                }
                // Append file segment onto end of file string
                file += fileSegment;
            }

            centralInout.close();
            central.close();

            System.out.printf("Sending \"%s\" back to client!\n", file);

            // Send compiled file back to client
            inout.println(file);
        } catch (Exception e) {
            System.err.println(e);
            inout.println(INTERNALSERVERERROR);
        }
    }

    /**
     * Leaf server receives a GET request for a segment
     * 
     * @param inout Input & output for socket
     */
    private void getSegment(SocketIO inout) {
        try {
            String name = inout.readLine();
            int segment = Integer.parseInt(inout.readLine());

            System.out.println(String.format("GET segment(%s, %d)", name, segment));

            // Get file segment from local storage
            String data = readFile(String.format("./segments/%s/%d.txt", name, segment));

            // Send file segment back to requester
            inout.println(data);
        } catch (Exception e) {
            System.err.println(e);
            inout.println(INTERNALSERVERERROR);
        }
    }

    /**
     * Leaf server receives a POST request for a file
     * 
     * @param inout Input & output for socket
     */
    private void postFile(SocketIO inout) {
        try {
            String name = inout.readLine();
            String data = inout.readLine();

            System.out.println(String.format("POST file(%s, %s)", name, data));

            int numberOfCharacters = data.length();
            int numberOfPairs = (int) Math.floor((double) numberOfCharacters / (double) CHUNK_SIZE) + 1;

            System.out.printf("Breaking \"%s\" up into %d chunk(s)...\n", data, numberOfPairs);

            String hostsAndSegments = "";

            for (int i = 0; i < numberOfPairs; i++) {
                // String where character will be concatenated to and eventually sent to leaf
                String chunk = "";

                for (int x = 0; x < CHUNK_SIZE; x++) {
                    int currentCharacter = (i * CHUNK_SIZE) + x;
                    // Concatenate character onto chunk or add empty space if we ran out of
                    // characters from file to make sure chunk matches chunk size
                    chunk += currentCharacter >= data.length() ? ' ' : data.charAt(currentCharacter);
                }

                if (ENV.equals("DEV")) {
                    synchronized (leafServers) {
                        while (leafServers.isEmpty())
                            ;
                    }
                }

                while (true) {
                    // Choose random leaf to send to
                    int random = new Random().nextInt(leafServers.size());
                    Object randomKey = leafServers.keySet().toArray()[random];
                    IPAddress leafIP = leafServers.get(randomKey);

                    try {
                        // If random leaf is the current machine then write file to directory
                        if (leafIP.address.equals(IP)) {
                            writeFile(String.format("./segments/%s/%d.txt", name, i), chunk);
                        } else {
                            // Open connection with random leaf
                            Socket leaf = new Socket(leafIP.address, leafIP.port);
                            SocketIO leafInout = new SocketIO(leaf);

                            System.out.printf("Sending \"%s\" to %s...\n", chunk, leafIP.address);

                            // Send file segment to random leaf
                            String response = sendPostSegment(leafInout, name, i, chunk);

                            // If leaf couldn't store file segment then close connections and throw
                            // exception, should retry since in while loop
                            if (response.equals(INTERNALSERVERERROR)) {
                                leafInout.close();
                                leaf.close();
                                throw new Exception(String.format("Couldn't send to %s!", leafIP.address));
                            }

                            leafInout.close();
                            leaf.close();
                        }

                        // Concatenate location and segment sequence number to string of pairs to send
                        // to central server
                        hostsAndSegments += String.format("%s:%d,", leafIP.address, i);
                        break;
                    } catch (Exception e) {
                        System.err.println(e);
                        System.err.println("Retrying");
                    }
                }
            }

            // Establish connection with central
            Socket central = new Socket(centralServer.address, centralServer.port);
            SocketIO centralInout = new SocketIO(central);

            // Cut off last comma
            hostsAndSegments = hostsAndSegments.substring(0, hostsAndSegments.length() - 1);

            System.out.println(String.format("Sending locations, \"%s\", to central server @ %s...", hostsAndSegments,
                    centralServer.address));

            // If central returns a internal servicer error then close all connections and
            // throw exception
            if (sendPostLocations(centralInout, name, hostsAndSegments).equals(INTERNALSERVERERROR)) {
                centralInout.close();
                central.close();
                throw new Exception("Central server couldn't store host and segment addresses!");
            }

            centralInout.close();
            central.close();

            // Send ok status code back to client to let them know that their file was fully
            // saved
            inout.println(OK);
        } catch (Exception e) {
            System.err.println(e);
            inout.println(INTERNALSERVERERROR);
        }
    }

    /**
     * Leaf server receives a POST request for a segment
     * 
     * @param inout Input & output for socket
     */
    private void postSegment(SocketIO inout) {
        try {
            String name = inout.readLine();
            int segment = Integer.parseInt(inout.readLine());
            String data = inout.readLine();

            // Write segment to local storage
            writeFile(String.format("./segments/%s/%d.txt", name, segment), data);

            // Send ok status code back if it was successful
            inout.println(OK);
        } catch (Exception e) {
            System.err.println(e);
            inout.println(INTERNALSERVERERROR);
        }
    }

    /**
     * Read from file at {@param filePath}
     * 
     * @param filePath
     * @throws Exception
     * @returns Data from file
     */
    private String readFile(String filePath) throws Exception {
        File file = new File(filePath);
        Scanner reader = new Scanner(file);

        String data = reader.nextLine();

        reader.close();
        return data;
    }

    /**
     * Write {@param data} to file at {@param filePath}
     * 
     * @param filePath
     * @param data
     * @throws Exception
     */
    private void writeFile(String filePath, String data) throws Exception {
        File file = new File(filePath);
        file.getParentFile().mkdirs();

        FileWriter fileWriter = new FileWriter(file);
        PrintWriter printWriter = new PrintWriter(fileWriter);

        System.out.printf("Writing \"%s\" to myself at %s\n", data, filePath);

        printWriter.write(data);

        printWriter.close();
        fileWriter.close();
    }

    /**
     * Router of all endpoints on server
     * 
     * @param inout    Input & output for socket
     * @param method   GET/POST
     * @param endpoint Name of endpoint to hit {@code (/endpoint)}
     */
    private void router(SocketIO inout, String method, String endpoint) {
        if (method.contains("GET")) {
            switch (endpoint) {
                case "/file":
                    this.getFile(inout);
                    break;
                case "/segment":
                    this.getSegment(inout);
                    break;
                default:
                    inout.println(BADREQUEST);
            }
        } else if (method.contains("POST")) {
            switch (endpoint) {
                case "/file":
                    this.postFile(inout);
                    break;
                case "/segment":
                    this.postSegment(inout);
                    break;
                default:
                    inout.println(BADREQUEST);
            }
        } else {
            inout.println(BADREQUEST);
        }
    }

    /**
     * Waits for client to ping an endpoint. Once interaction is over it waits for
     * another client to contact it
     */
    private void talkToClient() {
        while (true) {
            try {
                Socket client = leaf.accept();
                SocketIO inout = new SocketIO(client);

                String[] request = inout.readLine().split(" ");
                if (request.length != 2) {
                    inout.println(BADREQUEST);
                    continue;
                }
                String method = request[0];
                String endpoint = request[1];

                router(inout, method, endpoint);

                inout.close();
                client.close();
            } catch (Exception e) {
                System.err.println(e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Leaf leaf = new Leaf();
    }
}
