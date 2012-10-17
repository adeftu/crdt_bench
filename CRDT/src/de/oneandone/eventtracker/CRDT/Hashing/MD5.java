package de.oneandone.eventtracker.CRDT.Hashing;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5 {
	public long hash(String value) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] bytes = md.digest(value.getBytes("UTF-8"));
			return (bytes[0] & 0xFFL)
					| ((bytes[1] & 0xFFL) << 8)
					| ((bytes[2] & 0xFFL) << 16)
					| ((bytes[3] & 0xFFL) << 24)
					| ((bytes[4] & 0xFFL) << 32)
					| ((bytes[5] & 0xFFL) << 40)
					| ((bytes[6] & 0xFFL) << 48)
					| ((bytes[7] & 0xFFL) << 56);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return 0;
	}

}
