/**
* IP & port structure
* @author Clayton Cook
* @author Siona Beaudoin
*/
public class IPAddress {
    /** IP address */
    public String address;
    /** Port */
    public int port;

    /**
     * Initialize class attributes
     * @param address IP address
     * @param port Port
     * @throws Exception
     */
    public IPAddress(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public String toString() {
        return String.format("%s:%d", this.address, this.port);
    }
}
