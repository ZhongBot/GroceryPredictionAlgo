import java.util.*;

public class Customer {
	
	// track each grocery for customer
	List<BrandedGroceryItem> groceryTracker;
	
	// customer information
	String customerID;
	
	// predicted grocery list
	List<BrandedGroceryItem> predictedGroceryList;
	
	public Customer(String customerID) {
		groceryTracker = new ArrayList<BrandedGroceryItem>();
		this.customerID = customerID;
		predictedGroceryList = new ArrayList<BrandedGroceryItem>();
	}
	
	public void AddGroceryItem(BrandedGroceryItem item) {
		predictedGroceryList.add(item);
	}
	
	public void RemoveGroceryItem(BrandedGroceryItem item) {
		predictedGroceryList.remove(item);
	}
	
	public void SubmitGroceryList() {
	}
}
