import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.*;

public class CassandraHelper {
	public Cluster cluster;
	public Session session;
	
	public CassandraHelper() {
		String contactPoint = "104.236.229.162";
		String keyspace = "fydp";
		cluster = Cluster.builder().addContactPoint(contactPoint).build();
		session = cluster.connect("fydp");
	}
	
	public void Shutdown() {
		cluster.close();
		session.close();
	}
}
