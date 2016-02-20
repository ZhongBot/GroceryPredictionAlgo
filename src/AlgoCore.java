import java.util.*;
import com.datastax.driver.core.*;
import org.joda.time.DateTime;
import org.joda.time.Days;

public class AlgoCore {
	List<Customer> customerList = new ArrayList<Customer>();
	CassandraHelper cassandraHelper = new CassandraHelper();

	public void InitializeCustomerList() {
		Iterator<Row> customerRowIter = cassandraHelper.SelectAll("users");
		while (customerRowIter.hasNext()) {
			Row row = customerRowIter.next();

			Customer customer = new Customer(row.getString("userid"));
			customer.InitializeGroceryTracker(cassandraHelper);

			customerList.add(customer);
		}
	}

	public double CalcInventory(Customer customer, BrandedGroceryItem brandedGroceryItem, int groceryIndex) {
		return customer.groceryTracker.get(groceryIndex).inventory;
	}
	
	public double CalcThreshold(Customer customer, BrandedGroceryItem brandedGroceryItem) {
		double cHW = 0.0;
		double cSW = 0.0;
		
		// calculate consumption based on point of purchase
		Iterator<Row> cSWIter = cassandraHelper.SelectAllFromUserProduct("consumption", customer.customerID, brandedGroceryItem.productID);
		if (cSWIter.hasNext()) {
			Row cSWRow = cSWIter.next();
			cSW = cSWRow.getInt("quantity_purchased") / (double)cSWRow.getInt("days_elapsed");
		}
		
		// calculate consumption based on consumption tracking hardware
		Iterator<Row> cHWIter = cassandraHelper.SelectAllFromUserProduct("consumption_freq", customer.customerID, brandedGroceryItem.productID);
		int cHWCount = 0;
		DateTime startDate = new DateTime();
		DateTime endDate = new DateTime();
		while (cHWIter.hasNext()) {
			Row cHWRow = cHWIter.next();
			
			if (cHWCount == 0) {
				startDate = new DateTime(cHWRow.getDate("timestamp"));
			}
			
			endDate = new DateTime(cHWRow.getDate("timestamp"));
			cHWCount++;
		}
		
		cHW = cHWCount / (double)Days.daysBetween(startDate, endDate).getDays();
		
		
		if (cSW == 0.0) {
			return cHW;
		} else if (cHW == 0.0) {
			return cSW;
		}
		
		return 0.5 * (cSW + cHW) * 2;
	}
	
	public double CalcSatisfaction(Customer customer, BrandedGroceryItem brandedGroceryItem) {
		return 0.0;
	}
	
	public double CalcLoyalty(Customer customer, BrandedGroceryItem brandedGroceryItem) {
		return 0.0;
	}
	
	public double CalcPromotion(Customer customer, BrandedGroceryItem brandedGroceryItem) {
		return 0.0;
	}
	
	public void main(String[] args) {
		int signal = 0;

		InitializeCustomerList();

		while (signal == 0) {
			for (Customer customer : customerList) {
				int groceryIndex = 0;
				for (BrandedGroceryItem brandedGroceryItem : customer.groceryTracker) {
					double inv = CalcInventory(customer, brandedGroceryItem, groceryIndex);
					double t = CalcThreshold(customer, brandedGroceryItem);
					double s = CalcSatisfaction(customer, brandedGroceryItem);
					double l = CalcLoyalty(customer, brandedGroceryItem);
					double p = CalcPromotion(customer, brandedGroceryItem);
					
					double purchaseInd = 1 / (inv - t) * s * (l + p);
					
					groceryIndex++;

				}
			}
			
		}
		cassandraHelper.Shutdown();
	}
}
