
public class ItemPair {
	int p1, p2;

	public ItemPair(int p1, int p2) {
		this.p1 = p1;
		this.p2 = p2;
	}

	public int GetP1() {
		return this.p1;
	}

	public int GetP2() {
		return this.p2;
	}

	public boolean Compare(ItemPair customerPair) {
		if (customerPair.p1 == this.p1 && customerPair.p2 == this.p2) {
			return true;
		} else if (customerPair.p1 == this.p2 && customerPair.p2 == this.p1) {
			return true;
		}

		return false;
	}

	public int GetCounterpart(int productID) {
		if (productID == this.p1) {
			return this.p2;
		} else if (productID == this.p2) {
			return this.p1;
		}

		return 0;
	}
	
	@Override
	public String toString() {
	    return this.p1 + "," + this.p2;
	}
}
