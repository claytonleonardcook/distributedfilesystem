import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;

// Create leaf server that keeps track of other leaves to communicate and a central server. It also waits for a client to connect to it to make requests
public class Leaf {
    private ServerSocket server;
    private Map<String, Socket> leafs;

    public Leaf() {
        Thread centralThread = new Thread(() -> this.listenToCentral());
        leafsThread.start();
        Thread clientThread = new Thread(() -> this.listenToClient());
        clientThread.start();
    }

    private void listenToCentral() {
        Socket central = new Socket(EnvironmentVariables.CENTRAL_IP, EnvironmentVariables.PORT);
        InputStream in = central.getInputStream();
        OutputStream out = central.getOutputStream();

        out.write("GETLEAFS".getBytes());

        byte[] buffer = new byte[1024];
        int bytesRead = in.read(buffer);
        String response = new String(buffer, 0, bytesRead);

        System.out.println(response);
    }

    private void listenToClient() {
        Socket client = new Socket(EnvironmentVariables.CENTRAL_IP);
        InputStream in = client.getInputStream();
        OutputStream out = client.getOutputStream();
    }

    public static void main(String[] args) throws Exception {
        Leaf leaf = new Leaf();
    }
}
