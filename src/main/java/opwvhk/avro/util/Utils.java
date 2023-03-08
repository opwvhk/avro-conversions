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

/**
 * Container class with various utilities that didn't fit elsewhere.
 */
public final class Utils {
	private static final Pattern UNDERSCORES_PLUS_FOLLOWERS = Pattern.compile("(?U)_([^_])([^_]*)");
	private static final Function<MatchResult, String> TWO_GROUPS_TO_UPPER_LOWER_CASE = m -> m.group(1).toUpperCase(Locale.ROOT) + m.group(2).toLowerCase(
			Locale.ROOT);
	private static final Pattern INITIAL_LETTER = Pattern.compile("(?U)^(\\p{L})");
	private static final Function<MatchResult, String> FIRST_GROUP_TO_UPPER_CASE = m -> m.group(1).toUpperCase(Locale.ROOT);
	// Initial capitals are all first capitals of a word, or capitalised abbreviation (followed by end-of-input, a capitalised word, or plural 's')
	private static final Pattern INITIAL_CAPITALS = Pattern.compile("(?U)^(\\p{Lu}(?!\\p{Lu})|\\p{Lu}+(?=$|\\p{Lu}\\p{Ll}|s))");
	private static final Function<MatchResult, String> FIRST_GROUP_TO_LOWER_CASE = m -> m.group(1).toLowerCase(Locale.ROOT);

	/**
	 * Create a message digest. Assumes that the given algorithm is validated, and throws an {@link IllegalArgumentException} if it doesn't exist.
	 *
	 * @param algorithm a message digest algorithm
	 * @return a message digest for the specified algorithm
	 */
	public static MessageDigest digest(String algorithm) {
		try {
			return MessageDigest.getInstance(algorithm);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalArgumentException("Unknown algorithm", e);
		}
	}

	/**
	 * Convert a name from snake case to lower camel case.
	 *
	 * @param name a name in snake case
	 * @return the same name in camel case, starting with a lower case letter
	 */
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

	/**
	 * Convert a name from snake case to upper camel case.
	 *
	 * @param name a name in snake case
	 * @return the same name in camel case, starting with a camel case letter
	 */
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

	/**
	 * In the given name, convert the first character to upper case.
	 *
	 * @param name a name
	 * @return the same name, but with the first character in upper case
	 */
	public static String initialCapital(String name) {
		return INITIAL_LETTER.matcher(name).replaceAll(FIRST_GROUP_TO_UPPER_CASE);
	}

	/**
	 * In the given name, convert the first character to lower case.
	 *
	 * @param name a name
	 * @return the same name, but with the first character in lower case
	 */
	public static String noInitialCapital(String name) {
		return INITIAL_CAPITALS.matcher(name).replaceAll(FIRST_GROUP_TO_LOWER_CASE);
	}

	/**
	 * Returns the first non-{@code null} argument, or {@code null} if all are {@code null}.
	 *
	 * @param values some values
	 * @return the first non-{@code null} value, or {@code null}
	 */
	@SafeVarargs
	public static <T> T first(T... values) {
		for (T value : values) {
			if (value != null) {
				return value;
			}
		}
		return null;
	}

	/**
	 * Truncate the input to at most {@code maxLength} characters, ending it with ellipsis if it was truncated.
	 *
	 * @param maxLength the maximum length of the result
	 * @param input     an input string
	 * @return the same input string, or a truncated input string with appended ellipsis exactly {@code maxLength} characters long
	 */
	public static String truncate(int maxLength, String input) {
		if (input == null) {
			return null;
		} else if (input.length() <= maxLength) {
			return input;
		} else {
			return input.substring(0, maxLength - 1) + "…";
		}
	}

	/**
	 * <p>An implementation for {@link Object#equals(Object)} that guards against infinite recursion.</p>
	 *
	 * <p>To be equal, the objects must have the same class (subclasses don't count) and all accessors must be equal as well. When doing such a check for a
	 * recursive object definition, a stack overflow will occur. This method guards against that.</p>
	 *
	 * @param self      the object to compare
	 * @param other     the object to compare to
	 * @param accessors functions (usually lambdas) that yield the field values to compare for equality
	 * @return {@code true} if the objects have the same type, and the results from {@code accessors} are equal
	 * @see #nonRecursive(String, Object, Object, Object, Supplier) nonRecursive(String, Object, Object, U, Supplier&lt;U&gt;)
	 */
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

	/**
	 * <p>An implementation for {@link Object#hashCode()} that guards against infinite recursion.</p>
	 *
	 * <p>To calculate the hash, the hashcode of all provided field values is calculated. When doing such a calculation on a recursive object definition, a
	 * stack overflow will occur. This method guards against that.</p>
	 *
	 * @param self        the object whose hash to compare
	 * @param fieldValues the field values to include in the hash
	 * @return the calculated hashcode
	 * @see #nonRecursive(String, Object, Object, Supplier) nonRecursive(String, Object, U, Supplier&lt;U&gt;)
	 */
	public static <T> int recursionSafeHashCode(T self, Object... fieldValues) {
		return Utils.nonRecursive("recursionSafeHashCode", requireNonNull(self), 17, () -> Objects.hash(fieldValues));
	}

	private static final ThreadLocal<Map<String, Set<Seen>>> SEEN = ThreadLocal.withInitial(HashMap::new);

	/**
	 * <p>Calculate a result for a possibly infinitely recursive algorithm.</p>
	 *
	 * <p>This method detects if the same caller was used for the same algorithm while calculating a result.
	 * This happens when a calculation needs its own result as input, and normally results in a stack overflow. This method cuts the infinite recursion by
	 * substituting a default value instead.</p>
	 *
	 * @param algorithm     the name of the infinitely recursive algorithm
	 * @param caller        the caller object; used to break recursion
	 * @param defaultResult the result to provide for a caller if the result for the caller is still being calculated
	 * @param recursiveTask a calculation to provide a result for the caller; can call itself via our calling method
	 * @return the result of the calculation
	 */
	public static <T> T nonRecursive(String algorithm, Object caller, T defaultResult, Supplier<T> recursiveTask) {
		return nonRecursive(algorithm, caller, null, defaultResult, recursiveTask);
	}

	/**
	 * <p>Calculate a result for a possibly infinitely recursive algorithm.</p>
	 *
	 * <p>This method detects if the same caller/differentiator was used for the same algorithm while calculating a result.
	 * This happens when a calculation needs its own result as input, and normally results in a stack overflow. This method cuts the infinite recursion by
	 * substituting a default value instead.</p>
	 *
	 * @param algorithm      the name of the infinitely recursive algorithm
	 * @param caller         the caller object; used to break recursion
	 * @param differentiator a secondary object; used to break recursion
	 * @param defaultResult  the result to provide for a caller if the result for the caller is still being calculated
	 * @param recursiveTask  a calculation to provide a result for the caller; can call itself via our calling method
	 * @return the result of the calculation
	 */
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
