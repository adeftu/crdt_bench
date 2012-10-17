package de.oneandone.eventtracker.CRDT.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TrafficStats {
	public static class BytesStats {
		public long totalTxBytes;
		public long totalRxBytes;
	}

	public static BytesStats getBytesStats(String iface) throws IOException {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("/proc/net/dev"));
			String line;
			while ((line = br.readLine()) != null) {
				String[] tokens = line.trim().split(":|[ ]+");
				if (tokens[0].equals(iface)) {
					BytesStats bs = new BytesStats();
					bs.totalRxBytes = Long.parseLong(tokens[1]);
					bs.totalTxBytes = Long.parseLong(tokens[9]);
					return bs;
				}
			}
			throw new IllegalArgumentException(iface + " not found");
		} finally {
			if (br != null) 
				br.close();
		}
	}
}
