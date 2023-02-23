package opwvhk.avro.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public final class Utils {
	private static final Pattern UNDERSCORES_PLUS_FOLLOWERS = Pattern.compile("(?U)_([^_])([^_]*)");
	private static final Function<MatchResult, String> TWO_GROUPS_TO_UPPER_LOWER_CASE = m -> m.group(1).toUpperCase(Locale.ROOT) + m.group(2).toLowerCase(
			Locale.ROOT);
	private static final Pattern INITIAL_LETTER = Pattern.compile("(?U)^(\\p{L})");
	private static final Function<MatchResult, String> FIRST_GROUP_TO_UPPER_CASE = m -> m.group(1).toUpperCase(Locale.ROOT);
	// Initial capitals are all first capitals of a word, or capitalised abbreviation (followed by end-of-input, a capitalised word, or plural 's')
	private static final Pattern INITIAL_CAPITALS = Pattern.compile("(?U)^(\\p{Lu}(?!\\p{Lu})|\\p{Lu}+(?=$|\\p{Lu}\\p{Ll}|s))");
	private static final Function<MatchResult, String> FIRST_GROUP_TO_LOWER_CASE = m -> m.group(1).toLowerCase(Locale.ROOT);

	public static MessageDigest digest(String algorithm) {
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

	public static String truncate(int maxLength, String input) {
		if (input == null) {
			return null;
		} else if (input.length() <= maxLength) {
			return input;
		} else {
			return input.substring(0, maxLength - 1) + "…";
		}
	}

	@SafeVarargs
	public static <T> boolean recursionSafeEquals(T self, Object other, Function<T, Object>... accessors) {
		return Utils.nonRecursive("recursionSafeEquals", self, other, true, () -> {
			if (self == other) {
				return true;
			}
			Class<T> type = (Class<T>) self.getClass();
			if (other == null || other.getClass() != type) {
				return false;
			}
			for (Function<T, Object> accessor : accessors) {
				if (!Objects.deepEquals(accessor.apply(self), accessor.apply((T) other))) {
					return false;
				}
			}
			return true;
		});
	}

	public static <T> int recursionSafeHashCode(T self, Object... fieldValues) {
		return Utils.nonRecursive("recursionSafeHashCode", requireNonNull(self), 17, () -> Objects.hash(fieldValues));
	}

	private static final ThreadLocal<Map<String, Set<Seen>>> SEEN = ThreadLocal.withInitial(HashMap::new);

	public static <T> T nonRecursive(String algorithm, Object caller, T defaultResult, Supplier<T> recursiveTask) {
		return nonRecursive(algorithm, caller, null, defaultResult, recursiveTask);
	}

	public static <T> T nonRecursive(String algorithm, Object caller, Object differentiator, T defaultResult, Supplier<T> recursiveTask) {
		Map<String, Set<Seen>> seenMap = SEEN.get();
		boolean first = !seenMap.containsKey(algorithm);
		T result;
		try {
			Set<Seen> seen = seenMap.computeIfAbsent(algorithm, ignored -> new HashSet<>());
			Seen here = new Seen(caller, differentiator);
			if (seen.add(here)) {
				result = recursiveTask.get();
			} else {
				result = defaultResult;
			}
		} finally {
			if (first) {
				seenMap.remove(algorithm);
			}
		}
		return result;
	}

	private Utils() {
		// Utility class: no need to instantiate.
	}

	/**
	 * Simple class with equality check (!) on its contents, used to prevent infinite recursion.
	 */
	static class Seen {
		private final Object left;
		private final Object right;

		Seen(Object left, Object right) {
			this.left = left;
			this.right = right;
		}

		public boolean equals(Object o) {
			if (!(o instanceof Seen that)) {
				return false;
			}
			return this.left == that.left & this.right == that.right;
		}

		@Override
		public int hashCode() {
			return System.identityHashCode(left) + System.identityHashCode(right);
		}
	}
}
