import java.util.*;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.*;

public class CassandraHelper {
	public Cluster cluster;
	public Session session;

	public CassandraHelper(String contactPoint, String keyspace) {
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
		Statement statement = QueryBuilder.select().all().from(table).where(QueryBuilder.eq("userid", customerID));
		return session.execute(statement).iterator();
	}

	public Iterator<Row> SelectAllFromUserProduct(String table, String customerID, int productID) {
		Statement statement = QueryBuilder.select().all().from(table).where(QueryBuilder.eq("userid", customerID))
				.and(QueryBuilder.eq("product_id", productID));
		return session.execute(statement).iterator();
	}

	public Row SelectProduct(int productID) {
		Statement statement = QueryBuilder.select().all().from("products")
				.where(QueryBuilder.eq("product_id", productID));
		return session.execute(statement).one();
	}

	public void InsertFromUserProduct(String table, String customerID, int productID, String column, double value) {
		Statement statement = QueryBuilder.insertInto(table).value("userid", customerID).value("product_id", productID)
				.value(column, value);
		session.execute(statement);
	}

	public void InsertPurchaseInd(String customerID, int productID, double purchaseInd) {

		Statement statement = QueryBuilder.insertInto("purchase_ind").value("userid", customerID)
				.value("product_id", productID).value("purchase_ind", purchaseInd)
				.value("compute_time", System.currentTimeMillis());
		session.execute(statement);
	}

	public void InsertGroceryList(String customerID, List<Integer> productList) {
		Statement statement = QueryBuilder.insertInto("shoppinglists").value("sl_id", QueryBuilder.uuid())
				.value("time", System.currentTimeMillis()).value("userid", customerID)
				.value("products", productList).value("name", "Predicted Grocery List").value("autogenerate", true);
		session.execute(statement);
	}

	public void DeleteFromUserProduct(String table, String customerID, int productID) {
		Statement statement = QueryBuilder.delete().from(table).where(QueryBuilder.eq("userid", customerID))
				.and(QueryBuilder.eq("product_id", productID));
		session.execute(statement);
	}
}
