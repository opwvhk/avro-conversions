package opwvhk.avro.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

public final class Utils {
	private static final Pattern UNDERSCORES_PLUS_FOLLOWERS = Pattern.compile("(?U)_([^_])([^_]*)");
	private static final Function<MatchResult, String> TWO_GROUPS_TO_UPPER_LOWER_CASE = m -> m.group(1).toUpperCase(Locale.ROOT) + m.group(2).toLowerCase(
			Locale.ROOT);
	private static final Pattern INITIAL_LETTER = Pattern.compile("(?U)^(\\p{L})");
	private static final Function<MatchResult, String> FIRST_GROUP_TO_UPPER_CASE = m -> m.group(1).toUpperCase(Locale.ROOT);
	// Initial capitals are all first capitals of a word, or capitalised abbreviation (followed by end-of-input, a capitalised word, or plural 's')
	private static final Pattern INITIAL_CAPITALS = Pattern.compile("(?U)^(\\p{Lu}(?!\\p{Lu})|\\p{Lu}+(?=$|\\p{Lu}\\p{Ll}|s))");
	private static final Function<MatchResult, String> FIRST_GROUP_TO_LOWER_CASE = m -> m.group(1).toLowerCase(Locale.ROOT);

	public static MessageDigest digest(@SuppressWarnings("SameParameterValue") String algorithm) {
		try {
			return MessageDigest.getInstance(algorithm);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalArgumentException("Unknown algorithm", e);
		}
	}

	public static String snakeToLowerCamelCase(String name) {
		String camelCase;
		if (name.contains("_")) {
			camelCase = UNDERSCORES_PLUS_FOLLOWERS.matcher(name).replaceAll(TWO_GROUPS_TO_UPPER_LOWER_CASE);
		} else {
			// No _: assume already camel case
			camelCase = name;
		}
		return noInitialCapital(camelCase);
	}

	public static String snakeToUpperCamelCase(String name) {
		String camelCase;
		if (name.contains("_")) {
			camelCase = UNDERSCORES_PLUS_FOLLOWERS.matcher(name).replaceAll(TWO_GROUPS_TO_UPPER_LOWER_CASE);
		} else {
			// No _: assume already camel case
			camelCase = name;
		}
		return initialCapital(camelCase);
	}

	public static String initialCapital(String name) {
		return INITIAL_LETTER.matcher(name).replaceAll(FIRST_GROUP_TO_UPPER_CASE);
	}

	public static String noInitialCapital(String name) {
		return INITIAL_CAPITALS.matcher(name).replaceAll(FIRST_GROUP_TO_LOWER_CASE);
	}

	@SafeVarargs
	public static <T> T first(T... values) {
		for (T value : values) {
			if (value != null) {
				return value;
			}
		}
		return null;
	}

	@SuppressWarnings("SameParameterValue")
	public static String truncate(int maxLength, String input) {
		if (input == null) {
			return null;
		} else if (input.length() <= maxLength) {
			return input;
		} else {
			return input.substring(0, maxLength - 1) + "…";
		}
	}

	private Utils() {
		// Utility class: no need to instantiate.
	}
}
