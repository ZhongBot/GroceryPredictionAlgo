import java.util.*;
import java.util.Map.Entry;

import com.datastax.driver.core.*;
import org.joda.time.DateTime;
import org.joda.time.Days;
import com.google.common.reflect.*;
import com.google.common.base.*;
import com.google.common.collect.*;

public class AlgoCore {
	// helper to access database
	CassandraHelper cassandraHelper = new CassandraHelper();

	// list of all customers
	List<Customer> customerList = new ArrayList<Customer>();

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
		for (BrandedGroceryItem item : customerList.get(0).GetGroceryTracker()) {

			if (categoricalProductMap.containsKey(item.GetCategory())) {
				categoricalProductMap.get(item.GetCategory()).add(item.GetProductID());
			} else {
				List<Integer> categoricalProductList = new ArrayList<Integer>();
				categoricalProductList.add(item.GetProductID());
				categoricalProductMap.put(item.GetCategory(), categoricalProductList);
			}

		}
	}

	public void InitializePrevGroceryLists(Customer customer) {
		Iterator<Row> groceryListRowIter = cassandraHelper.SelectAllFromUser("shoppinglists", customer.GetCustomerID());
		while (groceryListRowIter.hasNext()) {
			Row groceryListRow = groceryListRowIter.next();
			customer.GetCompleteGroceryList().addAll(groceryListRow.getList("products", Integer.class));
		}

		customer.InitializeGrocerySet();
	}

	public void CalcItemSimilarityIndex() {
		int maxFreq = 0;
		int totalItems = 0;

		Iterator<Row> groceryListRowIter = cassandraHelper.SelectAll("shoppinglists");
		Map<Integer, Integer> productFreqMap = new HashMap<Integer, Integer>();
		Map<ItemPair, Integer> productPairFreqMap = new HashMap<ItemPair, Integer>();

		while (groceryListRowIter.hasNext()) {
			Row groceryListRow = groceryListRowIter.next();
			for (int productID : groceryListRow.getList("products", Integer.class)) {
				if (productFreqMap.containsKey(productID)) {
					productFreqMap.put(productID, productFreqMap.get(productID) + 1);
				} else {
					productFreqMap.put(productID, 1);
				}
			}
		}

		Predicate<Integer> productFreqFilter = new Predicate<Integer>() {

			@Override
			public boolean apply(Integer arg0) {
				return (arg0 > 2);
			}
		};

		// prune
		Map<Integer, Integer> filteredFreqMap = Maps.filterValues(productFreqMap, productFreqFilter);

		Set<Integer> productSet = filteredFreqMap.keySet();

		// count item pairs that are not pruned out
		groceryListRowIter = cassandraHelper.SelectAll("shoppinglists");
		while (groceryListRowIter.hasNext()) {
			Row groceryListRow = groceryListRowIter.next();
			List<Integer> productList = groceryListRow.getList("products", Integer.class);

			for (int p1 : productList) {

				if (productSet.contains(p1)) {

					totalItems++;

					for (int p2 : productList) {

						if (productSet.contains(p2) && p1 != p2) {
							ItemPair itemPair;
							if (p1 < p2) {
								itemPair = new ItemPair(p1, p2);
							} else {
								itemPair = new ItemPair(p2, p1);
							}

							if (productPairFreqMap.containsKey(itemPair)) {
								productPairFreqMap.put(itemPair, productFreqMap.get(itemPair) + 1);
							} else {
								productPairFreqMap.put(itemPair, 1);
							}
						}
					}

				}
			}
		}

		for (int freqCount : productPairFreqMap.values()) {
			if (freqCount > maxFreq) {
				maxFreq = freqCount;
			}
		}

		// store calculated item similarity
		for (ItemPair itemPair : productPairFreqMap.keySet()) {
			double pairFreq = (double) productPairFreqMap.get(itemPair);
			double sim = 0.5 * pairFreq * (1.0 / maxFreq + 1.0 / totalItems);
			similarityIndexMap.put(itemPair, sim);
		}
	}

	public void CalcPurchaseInd(Customer customer) {
		int groceryIndex = 0;
		for (BrandedGroceryItem brandedGroceryItem : customer.GetGroceryTracker()) {
			double inv = CalcInventory(customer, brandedGroceryItem, groceryIndex);
			double t = CalcThreshold(customer, brandedGroceryItem);
			double s = CalcSatisfaction(customer, brandedGroceryItem);
			double l = CalcLoyalty(customer, brandedGroceryItem);
			double p = CalcPromotion(customer, brandedGroceryItem);

			double purchaseInd = 1 / (inv - t) * s * (l + p);

			brandedGroceryItem.SetPurchaseInd(purchaseInd);

			groceryIndex++;

		}

		// store purchase ind to database, update average, stdev
	}

	public double CalcInventory(Customer customer, BrandedGroceryItem brandedGroceryItem, int groceryIndex) {
		double inv = customer.GetGroceryTracker().get(groceryIndex).GetInventory();
		return inv;
	}

	public double CalcThreshold(Customer customer, BrandedGroceryItem brandedGroceryItem) {
		double cHW = 0.0;
		double cSW = 0.0;
		double t = 0.0;

		// calculate consumption based on point of purchase
		Iterator<Row> cSWRowIter = cassandraHelper.SelectAllFromUserProduct("consumption", customer.GetCustomerID(),
				brandedGroceryItem.GetProductID());
		if (cSWRowIter.hasNext()) {
			Row cSWRow = cSWRowIter.next();
			cSW = cSWRow.getInt("quantity_purchased") / (double) cSWRow.getInt("days_elapsed");
		}

		// calculate consumption based on consumption tracking hardware
		Iterator<Row> cHWRowIter = cassandraHelper.SelectAllFromUserProduct("consumption_freq", customer.GetCustomerID(),
				brandedGroceryItem.GetProductID());
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
				customer.GetCustomerID(), brandedGroceryItem.GetProductID());
		if (satisfactionRowIter.hasNext()) {
			Row satisfactionRow = satisfactionRowIter.next();
			sEMA = satisfactionRow.getDouble("s_ema");
		}

		// get latest satisfaction
		satisfactionRowIter = cassandraHelper.SelectAllFromUserProduct("satisfaction_current", customer.GetCustomerID(),
				brandedGroceryItem.GetProductID());
		if (satisfactionRowIter.hasNext()) {
			Row satisfactionRow = satisfactionRowIter.next();
			currentRating = satisfactionRow.getDouble("rating");
		}

		// insert into tabu list
		if (currentRating == 0) {
			customer.InsertTabu(brandedGroceryItem.GetProductID());
		}

		sEMA = currentRating * alpha + sEMA * (1 - alpha);

		// store latest ema
		cassandraHelper.InsertFromUserProduct("satisfaction", customer.GetCustomerID(), brandedGroceryItem.GetProductID(), sEMA);

		if (customer.GetProductTabuMap().keySet().contains(brandedGroceryItem.GetProductID())) {
			s = sEMA - customer.GetProductTabuMap().get(brandedGroceryItem.GetProductID()) / 8.0;
		} else {
			s = sEMA;
		}

		return s;
	}

	public double CalcLoyalty(Customer customer, BrandedGroceryItem brandedGroceryItem) {

		double l = 0.0;

		List<Integer> categoricalProductList = categoricalProductMap.get(brandedGroceryItem.GetCategory());

		int categoryCount = 0;
		int loyalStreak = 0;
		int p = 0;
		int prevProductID = 0;
		for (int productID : customer.GetCompleteGroceryList()) {
			// only interested in the current category
			if (categoricalProductList.contains(productID)) {
				// check the longest streak that is not the current product
				if (productID == prevProductID && productID != brandedGroceryItem.GetProductID()) {
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

		double freq = (double) Collections.frequency(customer.GetCompleteGroceryList(), brandedGroceryItem.GetProductID());
		l = (freq + p / 2) / (double) categoryCount;

		return l;
	}

	public double CalcPromotion(Customer customer, BrandedGroceryItem brandedGroceryItem) {
		double p = 1 - brandedGroceryItem.GetPrice() / brandedGroceryItem.GetAvgPrice();
		if (p < 0.3) {
			return 0;
		} else {
			return p;
		}
	}

	public void ConstructHabitGroceryList(Customer customer) {
		for (BrandedGroceryItem item : customer.GetGroceryTracker()) {
			Iterator<Row> purchaseIndRowIter = cassandraHelper.SelectAllFromUserProduct("purchase_ind",
					customer.GetCustomerID(), item.GetProductID());
			int totalCount = 0;
			double total = 0.0;
			double sqTotal = 0.0;
			double avg = 0.0;
			double var = 0.0;
			double sd = 0.0;
			double pb = 0.0;

			while (purchaseIndRowIter.hasNext()) {
				Row purchaseIndRow = purchaseIndRowIter.next();
				total += purchaseIndRow.getDouble("purhcase_ind");
				sqTotal += Math.pow(purchaseIndRow.getDouble("purhcase_ind"), 2);
				totalCount++;
			}

			avg = total / (double) totalCount;
			var = sqTotal / totalCount - Math.pow(avg, 2);
			sd = Math.sqrt(var);

			pb = avg + sd;

			if (item.GetPurchaseInd() > pb) {
				customer.AddGroceryItem(item);
			}
		}

	}

	public void ExploreApriori(Customer customer) {
		Ordering<Map.Entry<ItemPair, Double>> similarityOrdering = new Ordering<Map.Entry<ItemPair, Double>>() {
			@Override
			public int compare(Entry<ItemPair, Double> arg0, Entry<ItemPair, Double> arg1) {
				return arg0.getValue().compareTo(arg1.getValue());
			}
		};

		List<Map.Entry<ItemPair, Double>> similarityIndexList = Lists.newArrayList(similarityIndexMap.entrySet());

		Collections.sort(similarityIndexList, similarityOrdering);

		for (Map.Entry<ItemPair, Double> itemPairEntry : similarityIndexList) {
			if (customer.GetPredictedGroceryList().contains(itemPairEntry.getKey().GetP1())
					&& !(customer.GetPredictedGroceryList().contains(itemPairEntry.getKey().GetP2()))) {
				if (customer.AddGroceryItem(customer.GetBrandedGroceryItem(itemPairEntry.getKey().GetP2()))) {
					// successfully added new explore item
					return;
				}

			} else if (customer.GetPredictedGroceryList().contains(itemPairEntry.getKey().GetP2())
					&& !(customer.GetPredictedGroceryList().contains(itemPairEntry.getKey().GetP1()))) {
				if (customer.AddGroceryItem(customer.GetBrandedGroceryItem(itemPairEntry.getKey().GetP1()))) {
					return;
				}
			}
		}
	}

	public void ExplorePSO(Customer c1) {
		double pr = 0.0;
		Random randGenerator = new Random();

		// make a copy
		Set<Integer> c1GrocerySet = c1.GetCompleteGrocerySet();

		for (Customer c2 : customerList) {
			Set<Integer> c2GrocerySet = c2.GetCompleteGrocerySet();

			if (c1 != c2) {
				int s1 = c1GrocerySet.size();
				int s2 = c2GrocerySet.size();

				// intersection of two sets
				c1GrocerySet.retainAll(c2GrocerySet);

				pr = 2.0 * c1GrocerySet.size() / (s1 + s2);
			}

			if (randGenerator.nextDouble() < pr) {
				// reset grocery sets
				c1GrocerySet = c1.GetCompleteGrocerySet();
				c2GrocerySet = c2.GetCompleteGrocerySet();

				// items in c2 not in c2
				c2GrocerySet.removeAll(c1GrocerySet);
				List<Integer> c2GroceryList = new ArrayList<Integer>(c2GrocerySet);
				Collections.shuffle(c2GroceryList);

				// add a random item in c2 not in c1
				if (c1.AddGroceryItem(c1.GetBrandedGroceryItem(c2GroceryList.get(0)))) {
					return;
				}
			}
		}

	}

	public void main(String[] args) {
		int signal = 0;

		InitializeCustomerList();
		InitializeProductMap();

		CalcItemSimilarityIndex();

		while (signal == 0) {
			for (Customer customer : customerList) {

				InitializePrevGroceryLists(customer);
				CalcPurchaseInd(customer);
				ConstructHabitGroceryList(customer);
				ExploreApriori(customer);
				ExplorePSO(customer);

			}

		}
		cassandraHelper.Shutdown();
	}
}
