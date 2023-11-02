import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class SocketIO {
    BufferedReader in;
    PrintWriter out;

    public SocketIO(Socket socket) throws Exception {
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        out = new PrintWriter(socket.getOutputStream(), true);
    }

    public String readLine() throws Exception {
        return in.readLine();
    }

    
    public <T> void println(T x) {
        out.println(x);
    }

    public void close() throws Exception {
        this.in.close();
        this.out.close();
    }
}
