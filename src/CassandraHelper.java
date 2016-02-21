import java.util.*;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.*;

public class CassandraHelper {
	public Cluster cluster;
	public Session session;

	public CassandraHelper() {
		String contactPoint = "104.236.229.162";
		String keyspace = "fydp";
		cluster = Cluster.builder().addContactPoint(contactPoint).build();
		session = cluster.connect(keyspace);
	}

	public void Shutdown() {
		cluster.close();
		session.close();
	}

	public Iterator<Row> SelectAll(String table) {
		Statement statement = QueryBuilder.select().all().from(table);
		return session.execute(statement).iterator();
	}

	public Iterator<Row> SelectAllFromUser(String table, String customerID) {
		Statement statement = QueryBuilder.select().all().from(table).where(QueryBuilder.eq("user_id", customerID));
		return session.execute(statement).iterator();
	}

	public Iterator<Row> SelectAllFromUserProduct(String table, String customerID, int productID) {
		Statement statement = QueryBuilder.select().all().from(table).where(QueryBuilder.eq("user_id", customerID));
		return session.execute(statement).iterator();
	}

	public Row SelectProduct(int productID) {
		Statement statement = QueryBuilder.select().all().from("products")
				.where(QueryBuilder.eq("product_id", productID));
		return session.execute(statement).one();
	}

	public void InsertFromUserProduct(String table, String customerID, int productID, double value) {
		Statement statement = QueryBuilder.insertInto(table).value("user_id", customerID).value("product_id", productID)
				.value("s_ema", value);
		session.execute(statement);
	}
}
