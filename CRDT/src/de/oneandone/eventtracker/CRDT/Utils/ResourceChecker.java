package de.oneandone.eventtracker.CRDT.Utils;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import com.sun.management.UnixOperatingSystemMXBean;

public class ResourceChecker {
	public static long getOpenFileDescriptorCount() {
		OperatingSystemMXBean osStats = ManagementFactory.getOperatingSystemMXBean();
		if (osStats instanceof UnixOperatingSystemMXBean) {
			return ((UnixOperatingSystemMXBean) osStats).getOpenFileDescriptorCount();
		}
		return 0;
	}
}
