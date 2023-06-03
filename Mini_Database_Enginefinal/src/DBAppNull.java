import java.io.Serializable;

public class DBAppNull implements Serializable, Comparable {

    @Override
    public String toString() {
        return "Null";
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}