import java.util.*;
import com.datastax.driver.core.*;
import org.joda.time.DateTime;
import org.joda.time.Days;
import com.google.common.reflect.*;

public class AlgoCore {
	// helper to access database
	CassandraHelper cassandraHelper = new CassandraHelper();
	
	// list of all customers
	List<Customer> customerList = new ArrayList<Customer>();
	
	// grocery history for a single customer
	List<Integer> completeGroceryList = new ArrayList<Integer>();
	
	// map of all products for each category
	Map<String, List<Integer>> categoricalProductMap = new HashMap<String, List<Integer>>();
	
	// map of similarity index per each pair of item
	Map<ItemPair, Double> similarityIndexMap = new HashMap<ItemPair, Double>();

	public void InitializeCustomerList() {
		Iterator<Row> customerRowIter = cassandraHelper.SelectAll("users");
		while (customerRowIter.hasNext()) {
			Row row = customerRowIter.next();

			Customer customer = new Customer(row.getString("userid"));
			customer.InitializeGroceryTracker(cassandraHelper);

			customerList.add(customer);
		}
	}

	public void InitializeProductMap() {
		// get all product id for each category
		for (BrandedGroceryItem item : customerList.get(0).groceryTracker) {

			if (categoricalProductMap.containsKey(item.category)) {
				categoricalProductMap.get(item.category).add(item.productID);
			} else {
				List<Integer> categoricalProductList = new ArrayList<Integer>();
				categoricalProductList.add(item.productID);
				categoricalProductMap.put(item.category, categoricalProductList);
			}

		}
	}
	
	public void InitializePrevGroceryLists(Customer customer) {
		Iterator<Row> groceryListRowIter = cassandraHelper.SelectAllFromUser("shoppinglists", customer.customerID);
		while (groceryListRowIter.hasNext()) {
			Row groceryListRow = groceryListRowIter.next();
			completeGroceryList.addAll(groceryListRow.getList("products", Integer.class));
		}
	}
	
	public void CalcItemSimilarityIndex() {
		
	}

	public double CalcInventory(Customer customer, BrandedGroceryItem brandedGroceryItem, int groceryIndex) {
		double inv = customer.groceryTracker.get(groceryIndex).inventory;
		return inv;
	}

	public double CalcThreshold(Customer customer, BrandedGroceryItem brandedGroceryItem) {
		double cHW = 0.0;
		double cSW = 0.0;
		double t = 0.0;

		// calculate consumption based on point of purchase
		Iterator<Row> cSWRowIter = cassandraHelper.SelectAllFromUserProduct("consumption", customer.customerID,
				brandedGroceryItem.productID);
		if (cSWRowIter.hasNext()) {
			Row cSWRow = cSWRowIter.next();
			cSW = cSWRow.getInt("quantity_purchased") / (double) cSWRow.getInt("days_elapsed");
		}

		// calculate consumption based on consumption tracking hardware
		Iterator<Row> cHWRowIter = cassandraHelper.SelectAllFromUserProduct("consumption_freq", customer.customerID,
				brandedGroceryItem.productID);
		int cHWCount = 0;
		DateTime startDate = new DateTime();
		DateTime endDate = new DateTime();
		while (cHWRowIter.hasNext()) {
			Row cHWRow = cHWRowIter.next();

			if (cHWCount == 0) {
				startDate = new DateTime(cHWRow.getDate("timestamp"));
			}

			endDate = new DateTime(cHWRow.getDate("timestamp"));
			cHWCount++;
		}

		cHW = cHWCount / (double) Days.daysBetween(startDate, endDate).getDays();

		if (cSW == 0.0) {
			return cHW;
		} else if (cHW == 0.0) {
			return cSW;
		}

		t = 0.5 * (cSW + cHW) * 2;
		return t;
	}

	public double CalcSatisfaction(Customer customer, BrandedGroceryItem brandedGroceryItem) {
		double alpha = 2.0 / (5.0 + 1.0);
		double sEMA = 0.0;
		double currentRating = 0.0;
		double s = 0.0;

		// get previous ema
		Iterator<Row> satisfactionRowIter = cassandraHelper.SelectAllFromUserProduct("satisfaction",
				customer.customerID, brandedGroceryItem.productID);
		if (satisfactionRowIter.hasNext()) {
			Row satisfactionRow = satisfactionRowIter.next();
			sEMA = satisfactionRow.getDouble("s_ema");
		}

		// get latest satisfaction
		satisfactionRowIter = cassandraHelper.SelectAllFromUserProduct("satisfaction_current", customer.customerID,
				brandedGroceryItem.productID);
		if (satisfactionRowIter.hasNext()) {
			Row satisfactionRow = satisfactionRowIter.next();
			currentRating = satisfactionRow.getDouble("rating");
		}

		// insert into tabu list
		if (currentRating == 0) {
			customer.InsertTabu(brandedGroceryItem.productID);
		}

		sEMA = currentRating * alpha + sEMA * (1 - alpha);

		// store latest ema
		cassandraHelper.InsertFromUserProduct("satisfaction", customer.customerID, brandedGroceryItem.productID, sEMA);

		if (customer.productTabuMap.keySet().contains(brandedGroceryItem.productID)) {
			s = sEMA - customer.productTabuMap.get(brandedGroceryItem.productID) / 8.0;
		} else {
			s = sEMA;
		}

		return s;
	}

	public double CalcLoyalty(Customer customer, BrandedGroceryItem brandedGroceryItem) {
		
		double l = 0.0;
		
		List<Integer> categoricalProductList = categoricalProductMap.get(brandedGroceryItem.category);
		
		int categoryCount = 0;
		int loyalStreak = 0;
		int p = 0;
		int prevProductID = 0;
		for (int productID : completeGroceryList) {
			// only interested in the current category
			if (categoricalProductList.contains(productID)) {
				// check the longest streak that is not the current product
				if (productID == prevProductID && productID != brandedGroceryItem.productID) {
					loyalStreak++;
				} else {
					if (loyalStreak > p) {
						p = loyalStreak;
					}
					loyalStreak = 0;
				}
				categoryCount++;
				prevProductID = productID;
			}
		}
		
		double freq = (double) Collections.frequency(completeGroceryList, brandedGroceryItem.productID);
		l = (freq + p / 2) / (double) categoryCount;
		
		return l;
	}

	public double CalcPromotion(Customer customer, BrandedGroceryItem brandedGroceryItem) {
		double p = 1 - brandedGroceryItem.price / brandedGroceryItem.avgPrice;
		if (p < 0.3) {
			return 0;
		} else {
			return p;
		}
	}

	public void main(String[] args) {
		int signal = 0;

		InitializeCustomerList();
		InitializeProductMap();
		
		CalcItemSimilarityIndex();
		

		while (signal == 0) {
			for (Customer customer : customerList) {
				int groceryIndex = 0;
				
				InitializePrevGroceryLists(customer);
				
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
