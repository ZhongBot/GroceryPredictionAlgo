import java.util.*;
import com.datastax.driver.core.*;

public class Customer {

	// track each grocery for customer
	List<BrandedGroceryItem> groceryTracker;

	// customer information
	String customerID;

	// predicted grocery list
	List<BrandedGroceryItem> predictedGroceryList;

	// map to track the highest purchase indicator for each category
	Map<String, BrandedGroceryItem> categoryBasedInd;
	
	// Tabu list for disliked items
	Map<Integer, Integer> productTabuMap;

	public Customer(String customerID) {
		groceryTracker = new ArrayList<BrandedGroceryItem>();
		this.customerID = customerID;
		predictedGroceryList = new ArrayList<BrandedGroceryItem>();
		categoryBasedInd =  new HashMap<String, BrandedGroceryItem>();
		productTabuMap = new HashMap<Integer, Integer>();
	}

	public void InitializeGroceryTracker(CassandraHelper cassandraHelper) {
		Iterator<Row> inventoryRowIter = cassandraHelper.SelectAllFromUser("inventory", this.customerID);

		while (inventoryRowIter.hasNext()) {
			Row inventory = inventoryRowIter.next();
			int productID = inventory.getInt("product_id");

			BrandedGroceryItem brandedGroceryItem = new BrandedGroceryItem(cassandraHelper.SelectProduct(productID));
			groceryTracker.add(brandedGroceryItem);
		}
	}

	public void AddGroceryItem(BrandedGroceryItem item) {

		if (categoryBasedInd.containsKey(item.category)) {
			if (categoryBasedInd.get(item.category).purchaseInd <= item.purchaseInd) {
				// remove lesser ind item
				RemoveGroceryItem(categoryBasedInd.get(item.category));

				// put higher ind item
				categoryBasedInd.put(item.category, item);
			} else {
				// nothing to be done
				return;
			}

		} else {
			// not item in list of that category, simply add
			categoryBasedInd.put(item.category, item);
		}

		predictedGroceryList.add(item);
	}

	public void RemoveGroceryItem(BrandedGroceryItem item) {
		predictedGroceryList.remove(item);
	}

	public void SubmitGroceryList() {
		List<Integer> finalGroceryList = new ArrayList<Integer>();
		for (BrandedGroceryItem predictedGroceryItem : predictedGroceryList) {
			finalGroceryList.add(predictedGroceryItem.productID);
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
}
