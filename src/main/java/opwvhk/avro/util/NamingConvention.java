/*
 * Copyright © Oscar Westra van Holthe - Kind
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package opwvhk.avro.util;

import org.jetbrains.annotations.NotNull;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>A class to provide <a href="https://en.wikipedia.org/wiki/Naming_convention_(programming)#Multiple-word_identifiers">naming conventions for multiple-word
 * identifiers</a>, like camel case, snake case, etc.</p>
 *
 * <p>It is up to the user of the class to ensure the applied naming convention makes sense: using e.g. camel case for a script that has no notion of
 * upper/lower case letters is not useful.</p>
 *
 * <h2>Algorithm</h2>
 *
 * <p>Casing is applied by first sanitising the text, and determining the list of words. Then the words are put together according to the selected style.</p>
 *
 * <p>This algorithm is somewhat opinionated: it does not make any special exceptions for acronyms. This is mostly in line with general guidelines, such as
 * from <a href="https://www.oracle.com/java/technologies/javase/codeconventions-namingconventions.html">Java/Oracle</a>
 * and <a href="https://learn.microsoft.com/en-us/dotnet/standard/design-guidelines/capitalization-conventions">Microsoft</a>, but ignores the Microsoft
 * exception for two-letter acronyms (like IO).</p>
 *
 * <p>Sanitation is done by creating a {@link Normalizer.Form#NFD canonical decomposition}, and then removing everything that is not in the
 * <a href="https://www.unicode.org/reports/tr44/#General_Category_Values">unicode categories</a> letter (L), number (N), space separator (Zs), connector
 * punctuation (Pc) or dash punctuation (Pd). This also removes accents. Words are then determined by splitting along spacing and punctuation.</p>
 *
 * <h2>Defined conventions</h2>
 *
 * <p>There are a number of capitalisation conventions predefined, combining various delimiters and combinations of upper and lower case, as listed below:</p>
 *
 * <table><caption>Capitalisation Conventions</caption><thead>
 *     <tr><th>Convention</th><th>Example</th></tr>
 * </thead><tbody>
 *     <tr><td>Pascal Case</td><td>PascalCase</td></tr>
 *     <tr><td>Camel Case</td><td>camelCase</td></tr>
 *     <tr><td>Snake Case</td><td>snake_case</td></tr>
 *     <tr><td>Kebab Case</td><td>kebab-case</td></tr>
 *     <tr><td>Pascal Snake Case</td><td>Pascal_Snake_Case</td></tr>
 *     <tr><td>Camel Snake Case</td><td>camel_Snake_Case</td></tr>
 *     <tr><td>Screaming Snake Case</td><td>SCREAMING_SNAKE_CASE</td></tr>
 *     <tr><td>Train Case</td><td>Train-Case</td></tr>
 *     <tr><td>Cobol Case</td><td>COBOL-CASE</td></tr>
 * </tbody></table>
 *
 * <p>Note that there is no predefined convention for (upper) flat case. The reason is that they are not reversible. The other conventions can be applied in any
 * sequence, and the last one deterministically determines the result. If any convention in between uses flat case, this is no longer true.</p>
 *
 * @see <a href="https://en.wikipedia.org/wiki/Naming_convention_(programming)#Multiple-word_identifiers">Naming conventions for multiple-word identifiers</a>
 * @see <a href="https://www.oracle.com/java/technologies/javase/codeconventions-namingconventions.html">Java naming convensions</a>
 * @see <a href="https://learn.microsoft.com/en-us/dotnet/standard/design-guidelines/capitalization-conventions">Microsoft capitalization conventions</a>
 * @see <a href="https://www.unicode.org/reports/tr44/#General_Category_Values">Unicode annex #44, General Category Values</a>
 */
public class NamingConvention {
	/**
	 * Pascal Case: capitalized words without delimiter.
	 */
	public static final NamingConvention PASCAL_CASE = new NamingConvention("", WordCase.CAPITALIZED, WordCase.CAPITALIZED);
	/**
	 * Camel Case: lowercase first word followed by capitalized words, without delimiter.
	 */
	public static final NamingConvention CAMEL_CASE = new NamingConvention("", WordCase.LOWER_CASE, WordCase.CAPITALIZED);
	/**
	 * Snake Case: lowercase words, separated by underscores.
	 */
	public static final NamingConvention SNAKE_CASE = new NamingConvention("_", WordCase.LOWER_CASE, WordCase.LOWER_CASE);
	/**
	 * Kebab Case: lowercase words, separated by hyphens.
	 */
	public static final NamingConvention KEBAB_CASE = new NamingConvention("-", WordCase.LOWER_CASE, WordCase.LOWER_CASE);
	/**
	 * Pascal Snake Case: capitalized words, separated by underscores.
	 */
	public static final NamingConvention PASCAL_SNAKE_CASE = new NamingConvention("_", WordCase.CAPITALIZED, WordCase.CAPITALIZED);
	/**
	 * Camel Snake Case: lowercase first word followed by capitalized words, separated by underscores.
	 */
	public static final NamingConvention CAMEL_SNAKE_CASE = new NamingConvention("_", WordCase.LOWER_CASE, WordCase.CAPITALIZED);
	/**
	 * Screaming Snake Case: uppercase words, separated by underscores.
	 */
	public static final NamingConvention SCREAMING_SNAKE_CASE = new NamingConvention("_", WordCase.UPPER_CASE, WordCase.UPPER_CASE);
	/**
	 * Train Case: capitalized words, separated by hyphens.
	 */
	public static final NamingConvention TRAIN_CASE = new NamingConvention("-", WordCase.CAPITALIZED, WordCase.CAPITALIZED);
	/**
	 * Cobol Case: uppercase words, separated by hyphens.
	 */
	public static final NamingConvention COBOL_CASE = new NamingConvention("-", WordCase.UPPER_CASE, WordCase.UPPER_CASE);
	/**
	 * Dummy naming convention that returns the given name as-is.
	 */
	public static final NamingConvention NULL = new NamingConvention(null, null, null) {
		@Override
		public String convert(String name) {
			return name;
		}
	};

	private final String delimiter;
	private final WordCase firstWord;
	private final WordCase otherWords;

	/**
	 * Create a naming convention for multiple-word identifiers. Combining an empty delimiter with {@link WordCase#LOWER_CASE} or
	 * {@link WordCase#UPPER_CASE} is discouraged, as the result cannot be converted to another naming convention.
	 *
	 * @param delimiter the word delimiter to use
	 * @param firstWord the capitalization for the first word
	 * @param otherWords the capitalization for the other words
	 */
	public NamingConvention(String delimiter, WordCase firstWord, WordCase otherWords) {
		this.delimiter = delimiter;
		this.firstWord = firstWord;
		this.otherWords = otherWords;
	}

	/**
	 * Convert a text/name to a name in this name case.
	 *
	 * @param name the name to convert
	 * @return the name in this name case
	 */
	public String convert(String name) {
		// First remove accents, extra punctuation, etc. Keep only letters, numbers, and dash & combining punctuation.
		String cleanName = NAME_CHARACTER_FILTER.matcher(Normalizer.normalize(name, Normalizer.Form.NFD)).replaceAll("");
		// if (cleanName.isEmpty() )

		// Then split by boundary characters, and determine the first non-empty word
		List<String> words = splitToWords(DELIMITER_BOUNDARY, cleanName);
		if (words.isEmpty()) {
			throw new IllegalArgumentException("The name contains no letters or numbers");
		} else if (words.size() == 1) {
			// The name contains no boundary characters: maybe it is camel case.
			words = splitToWords(CAMEL_BOUNDARY, cleanName);
		}

		StringBuilder buffer = new StringBuilder((int) (name.length() * 1.2f));
		Iterator<String> iterator = words.iterator();
		buffer.append(firstWord.apply(iterator.next()));
		iterator.forEachRemaining(word -> buffer.append(delimiter).append(otherWords.apply(word)));
		return buffer.toString();
	}

	/**
	 * Pattern to match anything that's not a letter, number or delimiter boundary.
	 */
	private static final Pattern NAME_CHARACTER_FILTER = Pattern.compile("[^\\p{L}\\p{N}\\p{Zs}\\p{Pd}\\p{Pc}]+");

	/**
	 * Pattern to match word boundaries using delimiters: any combination of spaces & dash/combining punctuation after a letter or number.
	 */
	private static final Pattern DELIMITER_BOUNDARY = Pattern.compile("[\\p{Zs}\\p{Pd}\\p{Pc}]+");
	/**
	 * Pattern to match any word boundary: the (zero-width) point between a lower- and uppercase letter, or any combination of spaces & punctuation.
	 */
	@SuppressWarnings("RegExpSimplifiable") // bug: the suggestion to remove [] from [\p{L}&&\P{Lu}] is wrong
	private static final Pattern CAMEL_BOUNDARY = Pattern.compile("(?<=[\\p{L}&&\\P{Lu}])(?=\\p{Lu})");

	@NotNull
	private List<String> splitToWords(Pattern wordBoundary, String text) {
		List<String> words = new ArrayList<>();
		Matcher matcher = wordBoundary.matcher(text);
		int start = 0;
		while (matcher.find()) {
			if (start < matcher.start()) {
				// Only add non-empty words
				words.add(text.substring(start, matcher.start()));
			}
			start = matcher.end();
		}
		if (start < text.length()) {
			// There's text remaining: add it
			words.add(text.substring(start));
		}
		return words;
	}

	/**
	 * Operator to apply "proper" to a name part.
	 */
	public enum WordCase implements UnaryOperator<String> {
		/**
		 * Convert the word to lower case.
		 */
		LOWER_CASE {
			@Override
			public String apply(String word) {
				return word.toLowerCase(Locale.ROOT);
			}
		},
		/**
		 * Convert the word to upper case.
		 */
		UPPER_CASE {
			@Override
			public String apply(String word) {
				return word.toUpperCase(Locale.ROOT);
			}
		},
		/**
		 * Convert the word to lower case, except the first character (convert that to upper case).
		 */
		CAPITALIZED {
			@Override
			public String apply(String word) {
				int firstCodePoint = word.codePointAt(0);
				int sizeOfFirstCharacter = Character.charCount(firstCodePoint);
				// Use toTitleCase instead of toUpperCase to properly handle digraphs.
				return Character.toString(Character.toTitleCase(firstCodePoint)) + word.substring(sizeOfFirstCharacter).toLowerCase(Locale.ROOT);
			}
		}
	}
}
