import java.net.InetAddress;
import java.util.HashMap;

/**
* Base class for all other server types to derive from. Stores environmental and global variables that all server types need access to.
* @author Clayton Cook
* @author Siona Beaudoin
*/
public abstract class DistributedFileSystem {
    /** Current machines IP address */
    final static String IP = "10.23.0.82";
    final static String ENV = "PROD";

    /** Collection of predefined status codes */
    final static String OK = "200",
            BADREQUEST = "400",
            NOTFOUND = "404",
            INTERNALSERVERERROR = "500";

    /** How big each segment of a file will be */
    final static int CHUNK_SIZE = 8;

    /** IP & port of central server */
    protected IPAddress centralServer;
    /** Collection/Lookup table for leafs based off their IP address */
    protected HashMap<String, IPAddress> leafServers;
    protected Runnable broadcast;
    protected int port;

    /**
     * Initialize class attributes, set central and leaf servers' locations
     * @throws Exception
     */
    public DistributedFileSystem() throws Exception {
        this.port = 3000;
        this.leafServers = new HashMap<String, IPAddress>();
        this.centralServer = new IPAddress("10.23.0.45", 4000);
        this.leafServers.put("10.23.2.102", new IPAddress("10.23.2.102", 3000));
        this.leafServers.put("10.23.0.82", new IPAddress("10.23.0.82", 3000));

        System.out.println(String.format("Running on %s", InetAddress.getLocalHost()));
    }

    /**
     * Send GET request for file
     * @param inout Input & output for socket
     * @param name Name of requested file
     * @return The contents of the requested file or a internal server error
     */
    protected static String sendGetFile(SocketIO inout, String name) {
        try {
            inout.println("GET /file");
            inout.println(name);

            return inout.readLine();
        } catch (Exception e) {
            return INTERNALSERVERERROR;
        }
    }

    /**
     * Send GET request for segment of file
     * @param inout Input & output for socket
     * @param name Name of requested file
     * @param segment Segment sequence number
     * @return The contents of the requested segment or a internal server error
     */
    protected static String sendGetSegment(SocketIO inout, String name, int segment) {
        try {
            inout.println("GET /segment");
            inout.println(name);
            inout.println(segment);

            return inout.readLine();
        } catch (Exception e) {
            return INTERNALSERVERERROR;
        }
    }

    /**
     * Send GET request for locations of file segments
     * @param inout Input & output for socket
     * @param fileLocations Name of file
     * @return String of comma seperated pairs of IP addresses and segment sequence numbers seperated by a colon
     */
    protected static String sendGetLocations(SocketIO inout, String fileLocations) {
        try {
            inout.println("GET /locations");
            inout.println(fileLocations);

            return inout.readLine();
        } catch (Exception e) {
            return INTERNALSERVERERROR;
        }
    }

    /**
     * Send POST request to store file
     * @param inout Input & output for socket
     * @param name Name of file
     * @param data Contents of file
     * @return Either a ok or internal server error status code
     */
    protected static String sendPostFile(SocketIO inout, String name, String data) {
        try {
            inout.println("POST /file");
            inout.println(name);
            inout.println(data);

            return inout.readLine();
        } catch (Exception e) {
            return INTERNALSERVERERROR;
        }
    }

    /**
     * Send POST request to store file segment
     * @param inout Input & output for socket
     * @param name Name of file
     * @param segment Segment sequence number
     * @param data Contents of file
     * @return Either a ok or internal server error status code
     */
    protected static String sendPostSegment(SocketIO inout, String name, int segment, String data) {
        try {
            inout.println("POST /segment");
            inout.println(name);
            inout.println(segment);
            inout.println(data);

            return inout.readLine();
        } catch (Exception e) {
            return INTERNALSERVERERROR;
        }
    }

    /**
     * Send POST request to store locations of file segments
     * @param inout Input & output for socket
     * @param fileName Name of file
     * @param fileContents String of comma seperated pairs of IP addresses and segment sequence numbers seperated by a colon
     * @return Either a ok or internal server error status code
     */
    protected static String sendPostLocations(SocketIO inout, String fileName, String fileContents) {
        try {
            inout.println("POST /locations");
            inout.println(fileName);
            inout.println(fileContents);

            return inout.readLine();
        } catch (Exception e) {
            return INTERNALSERVERERROR;
        }
    }
}
