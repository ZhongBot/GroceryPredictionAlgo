import java.util.*;
import com.datastax.driver.core.*;

public class Customer {

	// track each grocery for customer
	List<BrandedGroceryItem> groceryTracker;

	// customer information
	String customerID;

	// predicted grocery list
	List<BrandedGroceryItem> predictedGroceryList;

	// grocery history for a single customer
	List<Integer> completeGroceryList;

	// grocery set for a single customer
	Set<Integer> completeGrocerySet;

	// map to track the highest purchase indicator for each category
	Map<String, BrandedGroceryItem> categoryBasedInd;

	// Tabu list for disliked items
	Map<Integer, Integer> productTabuMap;

	public Customer(String customerID) {
		System.out.println("INFO - Creating customer " + customerID);
		groceryTracker = new ArrayList<BrandedGroceryItem>();
		this.customerID = customerID;
		predictedGroceryList = new ArrayList<BrandedGroceryItem>();
		completeGroceryList = new ArrayList<Integer>();
		categoryBasedInd = new HashMap<String, BrandedGroceryItem>();
		productTabuMap = new HashMap<Integer, Integer>();
	}

	public String GetCustomerID() {
		return this.customerID;
	}

	public List<BrandedGroceryItem> GetGroceryTracker() {
		return this.groceryTracker;
	}

	public List<Integer> GetCompleteGroceryList() {
		return this.completeGroceryList;
	}

	public Map<Integer, Integer> GetProductTabuMap() {
		return this.productTabuMap;
	}

	public List<BrandedGroceryItem> GetPredictedGroceryList() {
		return this.predictedGroceryList;
	}

	public Set<Integer> GetCompleteGrocerySet() {
		return this.completeGrocerySet;
	}

	public void InitializeGroceryTracker(CassandraHelper cassandraHelper) {
		Iterator<Row> inventoryRowIter = cassandraHelper.SelectAllFromUser("inventory", this.customerID);

		while (inventoryRowIter.hasNext()) {
			Row inventory = inventoryRowIter.next();
			int productID = inventory.getInt("product_id");

			BrandedGroceryItem brandedGroceryItem = new BrandedGroceryItem(cassandraHelper.SelectProduct(productID));
			brandedGroceryItem.inventory = (double) inventory.getLong("inventory");
			groceryTracker.add(brandedGroceryItem);

			// System.out.println("DEBUG - customer " + this.customerID + "
			// groceryTracker " + groceryTracker.toString());

		}
	}

	public boolean AddGroceryItem(BrandedGroceryItem item) {

		if (categoryBasedInd.containsKey(item.GetCategory())) {
			if (categoryBasedInd.get(item.GetCategory()).GetPurchaseInd() <= item.GetPurchaseInd()) {
				// remove lesser ind item
				RemoveGroceryItem(categoryBasedInd.get(item.GetCategory()));

				// put higher ind item
				categoryBasedInd.put(item.GetCategory(), item);
			} else {
				// nothing to be done
				return false;
			}

		} else {
			// not item in list of that category, simply add
			categoryBasedInd.put(item.GetCategory(), item);
		}

		System.out.println(
				"INFO - user " + this.customerID + " adding item " + item.productID + " to habit-based predicted list");
		predictedGroceryList.add(item);

		// successfully added the item
		return true;
	}

	public boolean AddExploreGroceryItem(BrandedGroceryItem item) {

		for (BrandedGroceryItem i : predictedGroceryList) {
			if (i.GetProductID() == item.GetProductID()) {
				// item already part of current list
				return false;
			}
		}

		if (productTabuMap.containsKey(item.GetProductID()) && productTabuMap.get(item.GetProductID()) > 0) {
			return false;
		}

		System.out.println("INFO - user " + this.customerID + " adding item " + item.productID
				+ " to explore-based predicted list");
		predictedGroceryList.add(item);

		// successfully added the item
		return true;
	}

	public void RemoveGroceryItem(BrandedGroceryItem item) {
		predictedGroceryList.remove(item);
	}

	public void SubmitGroceryList() {
		List<Integer> finalGroceryList = new ArrayList<Integer>();
		for (BrandedGroceryItem predictedGroceryItem : predictedGroceryList) {
			finalGroceryList.add(predictedGroceryItem.GetProductID());
		}

	}

	public void InsertTabu(int productID) {
		productTabuMap.put(productID, 3);
	}

	public void DecrementTabu() {
		for (Integer productID : productTabuMap.keySet()) {
			productTabuMap.put(productID, productTabuMap.get(productID) - 1);

			// no longer part of tabu, tenure used up
			if (productTabuMap.get(productID) <= 0) {
				productTabuMap.remove(productID);
			}
		}
	}

	public BrandedGroceryItem GetBrandedGroceryItem(int productID) {
		for (BrandedGroceryItem item : groceryTracker) {
			if (item.GetProductID() == productID) {
				return item;
			}
		}
		return null;
	}

	public boolean GroceryListContains(int productID) {
		for (BrandedGroceryItem brandedGroceryItem : predictedGroceryList) {
			if (brandedGroceryItem.GetProductID() == productID) {
				return true;
			}
		}
		return false;
	}

	public void InitializeGrocerySet() {
		completeGrocerySet = new HashSet<Integer>(this.completeGroceryList);
	}
}
