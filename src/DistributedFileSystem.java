import java.util.HashMap;

public abstract class DistributedFileSystem {
    final static String ENV = "PROD";
    final static String OK = "200",
            BADREQUEST = "400",
            NOTFOUND = "404",
            INTERNALSERVERERROR = "500";

    final static int CHUNK_SIZE = 8;

    protected IPAddress centralServer;
    protected HashMap<String, IPAddress> leafServers;
    protected Runnable broadcast;
    protected int port;

    public DistributedFileSystem() {
        this.port = 3000;
        this.leafServers = new HashMap<String, IPAddress>();
    }

    public DistributedFileSystem(int port) {
        this.port = port;
        this.leafServers = new HashMap<String, IPAddress>();
    }

    protected static String sendGetFile(SocketIO inout, String name) {
        try {
            inout.println("GET /file");
            inout.println(name);

            return inout.readLine();
        } catch (Exception e) {
            return INTERNALSERVERERROR;
        }
    }

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

    protected static String sendPostLocations(SocketIO inout, String code) {
        try {
            inout.println("POST /locations");
            inout.println(code);

            return inout.readLine();
        } catch (Exception e) {
            return INTERNALSERVERERROR;
        }
    }

    protected static String sendGetLocations(SocketIO inout, String fileLocations) {
        try {
            inout.println("GET /locations");
            inout.println(fileLocations);

            return inout.readLine();
        } catch (Exception e) {
            return INTERNALSERVERERROR;
        }
    }
}
