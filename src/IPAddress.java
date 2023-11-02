public class IPAddress {
    public String address;
    public int port;

    public IPAddress(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public String toString() {
        return String.format("%s:%d", this.address, this.port);
    }
}
