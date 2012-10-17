package de.oneandone.eventtracker.CRDT.ORset;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

/**
 * Class representing a topology for a replicated OR-set,
 * where the set is sharded across all stores in a cluster and
 * replicated to a number of clusters.
 * @author adeftu
 *
 */
public class Topology {
	private HashMap<String, HashMap<String, InetSocketAddress>> topology = new HashMap<String, HashMap<String,InetSocketAddress>>();
	
	/**
	 * Set the address of a store.
	 * @param rc		Replica cluster ID.
	 * @param rs		Replica store ID.
	 * @param address	Address of the store.
	 */
	public void set(String rc, String rs, InetSocketAddress address) {
		HashMap<String, InetSocketAddress> cluster = topology.get(rc);
		if (cluster == null) {
			HashMap<String, InetSocketAddress> newCluster = new HashMap<String, InetSocketAddress>();
			topology.put(rc, newCluster);
			cluster = newCluster;
		}
		cluster.put(rs, address);
	}
	

	/**
	 * Get the address of a store.
	 * @param rc	Replica cluster ID.
	 * @param rs	Replica store ID.
	 * @return		Address of the store.
	 */
	public InetSocketAddress get(String rc, String rs) {
		HashMap<String, InetSocketAddress> cluster = topology.get(rc);
		if (cluster == null)
			return null;
		return cluster.get(rs);
	}
	
	
	/**
	 * Get all cluster IDs.
	 */
	public Set<String> getClusterIDs() {
		return topology.keySet();
	}
	
	
	/**
	 * Get all store IDs from the given cluster ID.
	 */
	public Set<String> getStoreIDs(String rc) {
		return topology.get(rc).keySet();
	}
	
	
	/**
	 * Read the topology from an XML file.
	 * XML file format:
	 * <topology>
	 * 	<cluster id="A">
	 * 		<store id="x" ip="192.168.0.1" port=1337>
	 * 		<store id="y" ip="192.168.0.2" port=1337>
	 * 	</cluster>
	 *  <cluster id="B">
	 * 		<store id="z" ip="192.168.1.0" port=1337>
	 * 	</cluster>
	 * </topology>
	 */
	public void loadFromFile(String fileName) throws Exception {
		Document dom = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new File(fileName));
		org.w3c.dom.Element root = dom.getDocumentElement();
		root.normalize();
		NodeList clusters = root.getElementsByTagName("cluster");
		for (int i = 0; i < clusters.getLength(); ++i) {
			org.w3c.dom.Element cluster = (org.w3c.dom.Element) clusters.item(i);
			String rc = cluster.getAttribute("id");
			NodeList stores = cluster.getElementsByTagName("store");
			for (int j = 0; j < stores.getLength(); ++j) {
				org.w3c.dom.Element store = (org.w3c.dom.Element) stores.item(j);
				String rs = store.getAttribute("id");
				if (rc.contains(":") || rs.contains(":"))
					throw new Exception("IDs are not allowed to contain ':' character");
				set(rc, rs, new InetSocketAddress(store.getAttribute("ip"), Integer.parseInt(store.getAttribute("port"))));
			}
		}
	}
	
//	@Override
//	public String toString() {
//		StringBuilder sb = new StringBuilder();
//		for (String rc : topology.keySet()) {
//			sb.append(String.format("cluster '%s'\n", rc));
//			for (String rs : topology.get(rc).keySet()) {
//				sb.append(String.format("\tstore '%s': %s\n", rs, topology.get(rc).get(rs)));
//			}
//		}
//		if (sb.length() > 0);
//			sb.deleteCharAt(sb.length() - 1);
//		return sb.toString();
//	}
}
