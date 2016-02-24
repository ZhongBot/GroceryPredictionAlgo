import com.datastax.driver.core.*;

public class BrandedGroceryItem {
	// product descriptions
	int productID;
	double avgPrice;
	double price;
	String brand;
	String category;

	// customer-based inventory
	double inventory;

	// algo parameters
	double purchaseInd;
	double purchaseBarrier;

	public BrandedGroceryItem(Row groceryInfo) {
		productID = groceryInfo.getInt("product_id");
		avgPrice = groceryInfo.getInt("avg_price") / 100.0;
		price = groceryInfo.getInt("price") / 100.0;
		brand = groceryInfo.getString("brand");
		category = groceryInfo.getString("category");

		inventory = 0.0;

		purchaseInd = 0.0;
		purchaseBarrier = 0.0;
	}
	
	public int GetProductID() {
		return this.productID;
	}
	
	public double GetAvgPrice() {
		return this.avgPrice;
	}
	
	public double GetPrice() {
		return this.price;
	}

	public String GetBrand() {
		return this.brand;
	}
	
	public String GetCategory() {
		return this.category;
	}
	
	public double GetInventory() {
		return this.inventory;
	}
	
	public double GetPurchaseInd() {
		return this.purchaseInd;
	}
	
	public double GetPurchaseBarrier() {
		return this.purchaseBarrier;
	}
	
	public void CalcPurchaseBarrier() {
		purchaseBarrier = 0.0;
	}

	public void SetPurchaseInd(double purchaseInd) {
		this.purchaseInd = purchaseInd;
	}
}
