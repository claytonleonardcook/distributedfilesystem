import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;

public class Central {
    private Map<String, Socket> leafs;

    public Central() {
        Thread leafsThread = new Thread(() -> this.listenToLeafs());
        leafsThread.start();
    }

    private void listenToLeafs() {
        ServerSocket serverSocket = new ServerSocket(EnvironmentVariables.PORT);
        Socket server = serverSocket.accept();

        InputStream in = server.getInputStream();
        OutputStream out = server.getOutputStream();


        out.write("1,2,3".getBytes());
    }

    public static void main(String[] args) throws Exception {
        Central central = new Central();
    }
}
