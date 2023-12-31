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

public class Leaf extends DistributedFileSystem {
    private ServerSocket leaf;
    private Runnable centralBroadcast;

    public Leaf() throws Exception {
        this(3000);
    }

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
     * Leaf server receives a GET request for a file.
     *
     * @param in  Input from the socket sender
     * @param out Output to the socket sender
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
            Socket central = new Socket(centralServer.address, centralServer.port);
            SocketIO centralInout = new SocketIO(central);

            String allHostsAndSegments = sendGetLocations(centralInout, name);

            System.out.printf("Got \"%s\" from central server!\n", allHostsAndSegments);

            if (allHostsAndSegments.equals("500")) {
                centralInout.close();
                central.close();
                throw new Exception("Central server couldn't find segments for file!");
            }

            String[] hostsAndSegments = allHostsAndSegments.split(",");

            String file = "";

            for (String hostAndSegment : hostsAndSegments) {
                String host = hostAndSegment.split(":")[0];
                int segment = Integer.parseInt(hostAndSegment.split(":")[1]);

                if (leafServers.get(host) == null) {
                    centralInout.close();
                    central.close();
                    throw new Exception("Couldn't find leaf!");
                }

                String fileSegment = "";

                if (host.equals(IP)) {
                    fileSegment = readFile(String.format("./segments/%s/%d.txt", name, segment));
                    System.out.printf("Got \"%s\" from myself\n", fileSegment);
                } else {

                    System.out.printf("Requesting segment, %d, from %s\n", segment, host);

                    int port = leafServers.get(host).port;

                    Socket leaf = new Socket(host, port);
                    SocketIO leafInout = new SocketIO(leaf);

                    fileSegment = sendGetSegment(leafInout, name, segment);

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
                file += fileSegment;
            }

            centralInout.close();
            central.close();

            System.out.printf("Sending \"%s\" back to client!\n", file);

            inout.println(file);
        } catch (Exception e) {
            System.err.println(e);
            inout.println(INTERNALSERVERERROR);
        }
    }

    /**
     * Leaf server receives a GET request for a segment.
     *
     * @param in  Input from the socket sender
     * @param out Output to the socket sender
     */
    private void getSegment(SocketIO inout) {
        try {
            String name = inout.readLine();
            int segment = Integer.parseInt(inout.readLine());

            System.out.println(String.format("GET segment(%s, %d)", name, segment));

            String data = readFile(String.format("./segments/%s/%d.txt", name, segment));

            inout.println(data);
        } catch (Exception e) {
            System.err.println(e);
            inout.println(INTERNALSERVERERROR);
        }
    }

    /**
     * Leaf server receives a POST request for a file.
     *
     * @param in  Input from the socket sender
     * @param out Output to the socket sender
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
                String chunk = "";

                for (int x = 0; x < CHUNK_SIZE; x++) {
                    int currentCharacter = (i * CHUNK_SIZE) + x;
                    chunk += currentCharacter >= data.length() ? ' ' : data.charAt(currentCharacter);
                }

                if (ENV.equals("DEV")) {
                    synchronized (leafServers) {
                        while (leafServers.isEmpty())
                            ;
                    }
                }

                boolean wasSent = false;

                int random = new Random().nextInt(leafServers.size());
                Object randomKey = leafServers.keySet().toArray()[random];
                IPAddress leafIP = leafServers.get(randomKey);

                while (!wasSent) {
                    try {
                        if (leafIP.address.equals(IP)) {
                            writeFile(String.format("./segments/%s/%d.txt", name, i), chunk);
                        } else {
                            Socket leaf = new Socket(leafIP.address, leafIP.port);
                            SocketIO leafInout = new SocketIO(leaf);

                            System.out.printf("Sending \"%s\" to %s...\n", chunk, leafIP.address);
                            String response = sendPostSegment(leafInout, name, i, chunk);

                            if (response.equals(INTERNALSERVERERROR)) {
                                leafInout.close();
                                leaf.close();
                                throw new Exception(String.format("Couldn't send to %s!", leafIP.address));
                            }

                            leafInout.close();
                            leaf.close();
                        }
                        wasSent = true;
                        hostsAndSegments += String.format("%s:%d,", leafIP.address, i);
                        break;
                    } catch (Exception e) {
                        System.err.println(e);
                        System.err.println("Retrying");
                    }
                }
            }

            Socket central = new Socket(centralServer.address, centralServer.port);
            SocketIO centralInout = new SocketIO(central);

            hostsAndSegments = hostsAndSegments.substring(0, hostsAndSegments.length() - 1);

            System.out.println(String.format("Sending locations, \"%s\", to central server @ %s...", hostsAndSegments,
                    centralServer.address));

            if (sendPostLocations(centralInout, name, hostsAndSegments).equals(INTERNALSERVERERROR)) {
                centralInout.close();
                central.close();
                throw new Exception("Central server couldn't store host and segment addresses!");
            }

            centralInout.close();
            central.close();

            inout.println(OK);
        } catch (Exception e) {
            System.err.println(e);
            inout.println(INTERNALSERVERERROR);
        }
    }

    /**
     * Leaf server receives a POST request for a segment
     *
     * @param in  Input from the socket sender
     * @param out Output to the socket sender
     */
    private void postSegment(SocketIO inout) {
        try {
            String name = inout.readLine();
            int segment = Integer.parseInt(inout.readLine());
            String data = inout.readLine();

            writeFile(String.format("./segments/%s/%d.txt", name, segment), data);

            inout.println(OK);
        } catch (Exception e) {
            System.err.println(e);
            inout.println(INTERNALSERVERERROR);
        }
    }

    private String readFile(String filePath) throws Exception {
        File file = new File(filePath);
        Scanner reader = new Scanner(file);

        String data = reader.nextLine();

        reader.close();
        return data;
    }

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
