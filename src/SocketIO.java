import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
* Socket communication class that combines a PrintWriter and BufferedReader together to reduce code complexity
* @author Clayton Cook
* @author Siona Beaudoin
*/
public class SocketIO {
    protected BufferedReader in;
    protected PrintWriter out;

    /**
     * Initialize class attributes
     * @param socket Socket that needs to be written or read from
     * @throws Exception
     */
    public SocketIO(Socket socket) throws Exception {
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        out = new PrintWriter(socket.getOutputStream(), true);
    }

    /**
     * Reads a line of text. A line is considered to be terminated by any one of a line feed ('\n'), a carriage return ('\r'), a carriage return followed immediately by a line feed, or by reaching the end-of-file (EOF).
     * @return A String containing the contents of the line, not including any line-termination characters, or null if the end of the stream has been reached without reading any characters
     * @throws Exception
     */
    public String readLine() throws Exception {
        return in.readLine();
    }

    /**
     * Prints an Object and then terminates the line. This method calls at first String.valueOf(x) to get the printed object's string value, then behaves as though it invokes print(String) and then println().
     * @param x The {@code Object} to be printed.
     * @throws Exception
     */
    public void println(Object x) {
        out.println(x);
    }

    /**
     * Closes both streams and releases any system resources associated with them. Closing previously closed streams has no effect.
     * @throws Exception
     */
    public void close() throws Exception {
        this.in.close();
        this.out.close();
    }
}
