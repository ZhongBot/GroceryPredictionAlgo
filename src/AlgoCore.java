import java.beans.Customizer;
import java.util.*;
import java.util.Map.Entry;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.datastax.driver.core.*;
import org.joda.time.DateTime;
import org.joda.time.Days;
import com.google.common.reflect.*;
import com.google.common.base.*;
import com.google.common.collect.*;

public class AlgoCore {
	// helper to access database
	CassandraHelper cassandraHelper;

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

	public void InitializeCassandraHelper() {
		String contactPoint = "";
		String keyspace = "";
		Properties properties = new Properties();
		InputStream inputStream = null;

		try {

			inputStream = new FileInputStream("resources/config.properties");

			// load a properties file
			properties.load(inputStream);

			// get the property values
			contactPoint = properties.getProperty("database");
			keyspace = properties.getProperty("keyspace");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		cassandraHelper = new CassandraHelper(contactPoint, keyspace);
	}

	public void InitializeProductMap() {
		Customer customer = customerList.get(1);
		for (Customer c : customerList) {
			// System.out.println("DEBUG - customerID " + c.GetCustomerID());
			if (c.GetCustomerID().equals(customer.GetCustomerID())) {
				// System.out.println("DEBUG - selected correct customer");
				customer = c;
			}
		}

		// System.out.println("DEBUG - customer " + customer.customerID + "
		// groceryTracker "
		// + customer.GetGroceryTracker().toString());

		// get all product id for each category
		for (BrandedGroceryItem item : customer.GetGroceryTracker()) {

			if (categoricalProductMap.containsKey(item.GetCategory())) {
				categoricalProductMap.get(item.GetCategory()).add(item.GetProductID());
			} else {
				List<Integer> categoricalProductList = new ArrayList<Integer>();
				categoricalProductList.add(item.GetProductID());
				categoricalProductMap.put(item.GetCategory(), categoricalProductList);
			}

		}

		System.out.println("INFO - categoricalProductMap " + categoricalProductMap.toString());
	}

	public void InitializePrevGroceryLists() {
		for (Customer customer : customerList) {
			Iterator<Row> groceryListRowIter = cassandraHelper.SelectAllFromUser("shoppinglists",
					customer.GetCustomerID());
			while (groceryListRowIter.hasNext()) {
				Row groceryListRow = groceryListRowIter.next();
				customer.GetCompleteGroceryList().addAll(groceryListRow.getList("products", Integer.class));
			}

			System.out.println("INFO - customer " + customer.customerID + " completeGroceryList "
					+ customer.GetCompleteGroceryList().toString());

			customer.InitializeGrocerySet();

			System.out.println("INFO - customer " + customer.customerID + " grocerySet "
					+ customer.GetCompleteGrocerySet().toString());
		}
	}

	public void CalcItemSimilarityIndex() {
		int maxFreq = 0;
		int totalItems = 0;
		boolean found = false;

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

		System.out.println("INFO - productFreqMap " + productFreqMap.toString());

		Predicate<Integer> productFreqFilter = new Predicate<Integer>() {

			@Override
			public boolean apply(Integer arg0) {
				return (arg0 > 2);
			}
		};

		// prune
		Map<Integer, Integer> filteredFreqMap = Maps.filterValues(productFreqMap, productFreqFilter);

		System.out.println("INFO - filteredFreqMap " + filteredFreqMap.toString());

		Set<Integer> productSet = filteredFreqMap.keySet();

		System.out.println("INFO - productSet " + productSet.toString());

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

							for (ItemPair i : productPairFreqMap.keySet()) {
								if (i.Compare(itemPair)) {
									productPairFreqMap.put(i, productPairFreqMap.get(i) + 1);
									found = true;
								}
							}

							if (!found) {
								productPairFreqMap.put(itemPair, 1);
							}

							found = false;
						}
					}

				}
			}
		}

		System.out.println("INFO - totalItems " + totalItems);

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

		System.out.println("INFO - similarityIndexMap " + similarityIndexMap.toString());
	}

	public void CalcPurchaseInd(Customer customer) {
		int groceryIndex = 0;
		for (BrandedGroceryItem brandedGroceryItem : customer.GetGroceryTracker()) {
			double inv = CalcInventory(customer, brandedGroceryItem, groceryIndex);
			System.out.println("INFO - customer " + customer.customerID + " product "
					+ brandedGroceryItem.GetProductID() + " inventory " + inv);
			double t = CalcThreshold(customer, brandedGroceryItem);
			System.out.println("INFO - customer " + customer.customerID + " product "
					+ brandedGroceryItem.GetProductID() + " t " + t);
			double s = CalcSatisfaction(customer, brandedGroceryItem);
			System.out.println("INFO - customer " + customer.customerID + " product "
					+ brandedGroceryItem.GetProductID() + " s " + s);
			double l = CalcLoyalty(customer, brandedGroceryItem);
			System.out.println("INFO - customer " + customer.customerID + " product "
					+ brandedGroceryItem.GetProductID() + " l " + l);
			double p = CalcPromotion(customer, brandedGroceryItem);
			System.out.println("INFO - customer " + customer.customerID + " product "
					+ brandedGroceryItem.GetProductID() + " p " + p);

			double purchaseInd = 1 / Math.abs(inv - t) * s * (l + p);

			brandedGroceryItem.SetPurchaseInd(purchaseInd);

			cassandraHelper.InsertPurchaseInd(customer.GetCustomerID(), brandedGroceryItem.GetProductID(), purchaseInd);

			System.out.println("INFO - customer " + customer.customerID + " product "
					+ brandedGroceryItem.GetProductID() + " purchase indicator " + brandedGroceryItem.GetPurchaseInd());

			groceryIndex++;

		}
	}

	public double CalcInventory(Customer customer, BrandedGroceryItem brandedGroceryItem, int groceryIndex) {
		double inv = customer.GetGroceryTracker().get(groceryIndex).GetInventory();
		return inv;
	}

	public double CalcThreshold(Customer customer, BrandedGroceryItem brandedGroceryItem) {
		double cHW = 0.0;
		double cSW = 0.0;
		double d = 0.0;
		double t = 0.0;

		// calculate consumption based on point of purchase
		Iterator<Row> cSWRowIter = cassandraHelper.SelectAllFromUserProduct("consumption", customer.GetCustomerID(),
				brandedGroceryItem.GetProductID());
		if (cSWRowIter.hasNext()) {
			Row cSWRow = cSWRowIter.next();
			cSW = cSWRow.getInt("quantity_purchased") / (double) cSWRow.getInt("days_elapsed");
		}

		// calculate consumption based on consumption tracking hardware
		Iterator<Row> cHWRowIter = cassandraHelper.SelectAllFromUserProduct("consumption_freq",
				customer.GetCustomerID(), brandedGroceryItem.GetProductID());
		int cHWCount = 0;
		DateTime startDate = new DateTime();
		DateTime endDate = new DateTime();
		while (cHWRowIter.hasNext()) {
			Row cHWRow = cHWRowIter.next();

			if (cHWCount == 0) {
				startDate = new DateTime(cHWRow.getDate("date"));
			}

			endDate = new DateTime(cHWRow.getDate("date"));
			cHWCount++;
		}

		d = (double) Days.daysBetween(startDate, endDate).getDays();
		if (Double.compare(d, 0.0) != 0) {
			cHW = (cHWCount - 1) / d;
		}

		if (cSW == 0.0) {
			return cHW;
		} else if (cHW == 0.0) {
			return cSW;
		}

		t = 0.5 * (cSW + cHW) * 2;

		System.out.println("INFO - customer " + customer.customerID + " product " + brandedGroceryItem.GetProductID()
				+ " cSW " + cSW + " cHW " + cHW + " t " + t);

		return t;
	}

	public double CalcSatisfaction(Customer customer, BrandedGroceryItem brandedGroceryItem) {
		double alpha = 2.0 / (5.0 + 1.0);
		double sEMA = 0.0;
		double currentRating = -1.0;
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

			// process latest satisfaction
			cassandraHelper.InsertFromUserProduct("satisfaction_current", customer.GetCustomerID(),
					brandedGroceryItem.GetProductID(), "rating", 1.0);
		}

		if (currentRating >= 0.0) {
			// insert into tabu list
			if (currentRating == 0) {
				customer.InsertTabu(brandedGroceryItem.GetProductID());
			}

			sEMA = currentRating * alpha + sEMA * (1 - alpha);

			// store latest ema
			cassandraHelper.InsertFromUserProduct("satisfaction", customer.GetCustomerID(),
					brandedGroceryItem.GetProductID(), "s_ema", sEMA);

			if (customer.GetProductTabuMap().keySet().contains(brandedGroceryItem.GetProductID())) {
				s = sEMA - customer.GetProductTabuMap().get(brandedGroceryItem.GetProductID()) / 8.0;
			} else {
				s = sEMA;
			}
		} else {
			// rating did not change
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
				if ((categoryCount == 0 || productID == prevProductID)
						&& productID != brandedGroceryItem.GetProductID()) {
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

		double freq = (double) Collections.frequency(customer.GetCompleteGroceryList(),
				brandedGroceryItem.GetProductID());

		if (categoryCount != 0) {
			l = (freq + p / 2) / (double) categoryCount;
		}

		System.out.println("INFO - customer " + customer.customerID + " product " + brandedGroceryItem.GetProductID()
				+ " freq " + freq + " categoryCount " + categoryCount);

		return l;
	}

	public double CalcPromotion(Customer customer, BrandedGroceryItem brandedGroceryItem) {
		double p = 1.0 - brandedGroceryItem.GetPrice() / brandedGroceryItem.GetAvgPrice();

		if (p < 0.3) {
			return 0;
		} else {
			return p;
		}
	}

	public void CalcPurchaseBarrier(Customer customer, BrandedGroceryItem item) {
		int totalCount = 0;
		double total = 0.0;
		double sqTotal = 0.0;
		double avg = 0.0;
		double var = 0.0;
		double sd = 0.0;
		double pb = 0.0;

		Iterator<Row> purchaseIndRowIter = cassandraHelper.SelectAllFromUserProduct("purchase_ind",
				customer.GetCustomerID(), item.GetProductID());

		while (purchaseIndRowIter.hasNext()) {
			Row purchaseIndRow = purchaseIndRowIter.next();
			total += purchaseIndRow.getDouble("purhcase_ind");
			sqTotal += Math.pow(purchaseIndRow.getDouble("purhcase_ind"), 2);
			totalCount++;
		}

		if (totalCount > 0) {
			avg = total / (double) totalCount;
			var = sqTotal / totalCount - Math.pow(avg, 2);
			sd = Math.sqrt(var);
		}

		pb = avg + sd;
		item.SetPurchaseBarrier(pb);

		System.out.println("INFO - customer " + customer.customerID + " product " + item.productID + " purchase ind "
				+ item.GetPurchaseInd() + " pb " + item.GetPurchaseBarrier());

	}

	public void ConstructHabitGroceryList(Customer customer) {
		for (BrandedGroceryItem item : customer.GetGroceryTracker()) {

			// CalcPurchaseBarrier(customer, item);
			//
			// if (item.GetPurchaseInd() > item.GetPurchaseBarrier()) {
			// customer.AddGroceryItem(item);
			// }
			customer.AddGroceryItem(item);
		}

		System.out.println("INFO - customer " + customer.customerID + " habitual grocery list "
				+ customer.GetPredictedGroceryList());

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
				if (customer.AddExploreGroceryItem(customer.GetBrandedGroceryItem(itemPairEntry.getKey().GetP2()))) {
					// successfully added new explore item
					System.out.println("INFO - customer " + customer.customerID + " added apriori item "
							+ itemPairEntry.getKey().GetP2());
					return;
				}

			} else if (customer.GetPredictedGroceryList().contains(itemPairEntry.getKey().GetP2())
					&& !(customer.GetPredictedGroceryList().contains(itemPairEntry.getKey().GetP1()))) {
				if (customer.AddExploreGroceryItem(customer.GetBrandedGroceryItem(itemPairEntry.getKey().GetP1()))) {
					System.out.println("INFO - customer " + customer.customerID + " added apriori item "
							+ itemPairEntry.getKey().GetP1());
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

			if (c1.GetCustomerID() != c2.GetCustomerID()) {
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

				System.out.println("INFO - customer " + c1.customerID + " choose item from " + c2.customerID + " list "
						+ c2GrocerySet.toString());
				List<Integer> c2GroceryList = new ArrayList<Integer>(c2GrocerySet);
				Collections.shuffle(c2GroceryList);

				// add a random item in c2 not in c1
				if (c1.AddExploreGroceryItem(c1.GetBrandedGroceryItem(c2GroceryList.get(0)))) {
					System.out.println("INFO - customer " + c1.customerID + " PSO item " + c2GroceryList.get(0));
					return;
				}
			}
		}

	}

	public List<Customer> GetCustomerList() {
		return this.customerList;
	}

	public CassandraHelper GetCassandraHelper() {
		return this.cassandraHelper;
	}

	public static void main(String[] args) {
		int signal = 0;
		AlgoCore algoCore = new AlgoCore();

		algoCore.InitializeCassandraHelper();

		algoCore.InitializeCustomerList();

		algoCore.InitializeProductMap();

		algoCore.InitializePrevGroceryLists();

		algoCore.CalcItemSimilarityIndex();

		// continue while not terminated
		while (signal == 0) {
			for (Customer customer : algoCore.GetCustomerList()) {
				if (!customer.GetCustomerID().equals("p8zhao@uwaterloo.ca")) {
					// demo only generate for single user
					continue;
				}

				algoCore.CalcPurchaseInd(customer);
				algoCore.ConstructHabitGroceryList(customer);
				algoCore.ExploreApriori(customer);
				algoCore.ExplorePSO(customer);
				signal = 1;
			}

		}

		algoCore.GetCassandraHelper().Shutdown();
	}
}
