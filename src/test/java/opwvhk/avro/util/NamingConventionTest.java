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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NamingConventionTest {
	private static final String EXAMPLE = "multiple word identifier";
	private static final Map<NamingConvention, String> EXAMPLES = Map.of(
			NamingConvention.PASCAL_CASE, "MultipleWordIdentifier",
			NamingConvention.CAMEL_CASE, "multipleWordIdentifier",
			NamingConvention.SNAKE_CASE, "multiple_word_identifier",
			NamingConvention.KEBAB_CASE, "multiple-word-identifier",
			NamingConvention.PASCAL_SNAKE_CASE, "Multiple_Word_Identifier",
			NamingConvention.CAMEL_SNAKE_CASE, "multiple_Word_Identifier",
			NamingConvention.SCREAMING_SNAKE_CASE, "MULTIPLE_WORD_IDENTIFIER",
			NamingConvention.TRAIN_CASE, "Multiple-Word-Identifier",
			NamingConvention.COBOL_CASE, "MULTIPLE-WORD-IDENTIFIER"
	);

	@Test
	void validatePredefinedNamingConventions() {
		assertThat(NamingConvention.PASCAL_CASE.convert("Pascal Case")).isEqualTo("PascalCase");
		assertThat(NamingConvention.CAMEL_CASE.convert("Camel Case")).isEqualTo("camelCase");
		assertThat(NamingConvention.SNAKE_CASE.convert("Snake Case")).isEqualTo("snake_case");
		assertThat(NamingConvention.KEBAB_CASE.convert("Kebab Case")).isEqualTo("kebab-case");
		assertThat(NamingConvention.PASCAL_SNAKE_CASE.convert("Pascal Snake Case")).isEqualTo("Pascal_Snake_Case");
		assertThat(NamingConvention.CAMEL_SNAKE_CASE.convert("Camel Snake Case")).isEqualTo("camel_Snake_Case");
		assertThat(NamingConvention.SCREAMING_SNAKE_CASE.convert("Screaming Snake Case")).isEqualTo("SCREAMING_SNAKE_CASE");
		assertThat(NamingConvention.TRAIN_CASE.convert("Train Case")).isEqualTo("Train-Case");
		assertThat(NamingConvention.COBOL_CASE.convert("Cobol Case")).isEqualTo("COBOL-CASE");
	}

	@Test
	void verifyPredefinedNamingConventionsAreDeterministic() {
		List<NamingConvention> namingConventions = new ArrayList<>(EXAMPLES.keySet());
		Collections.shuffle(namingConventions);

		String name = EXAMPLE;
		for (NamingConvention namingConvention : namingConventions) {
			name = namingConvention.convert(name);
			assertThat(name).isEqualTo(EXAMPLES.get(namingConvention));
		}
	}

	@Test
	void validateWordCase() {
		assertThat(NamingConvention.WordCase.LOWER_CASE.apply("MiXeD")).isEqualTo("mixed");
		assertThat(NamingConvention.WordCase.UPPER_CASE.apply("MiXeD")).isEqualTo("MIXED");
		assertThat(NamingConvention.WordCase.CAPITALIZED.apply("MiXeD")).isEqualTo("Mixed");
	}

	@Test
	@SuppressWarnings("SpellCheckingInspection")
	void validateWordSplitting() {
		NamingConvention dummy = new NamingConvention(" ", NamingConvention.WordCase.CAPITALIZED, NamingConvention.WordCase.LOWER_CASE);

		// Text with accents, various dashes & spaces, and nonsense characters
		assertThat(dummy.convert("th↔︎Ïs—IS–a Sèn🛫ténçE")).isEqualTo("This is a sentence");
		// Greek text with accents, an underscore and various spaces / space marks ("Αυτή είναι μια πρόταση" translates to "This is a sentence")
		assertThat(dummy.convert("αυτή είΝαι_μΙα﹏ΠΡόταση")).isEqualTo("Αυτη ειναι μια προταση");
		// Text without dashes & spaces
		assertThat(dummy.convert("th↔︎ïsIsAnotherSèn🛫ténçe")).isEqualTo("This is another sentence");

		assertThatThrownBy(() -> dummy.convert("🛫  ﹏_ ↔︎")).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	void ensureTheNullConventionDoesNothing() {
		byte[] randomBytes = new byte[16];
		new Random().nextBytes(randomBytes);
		String randomString = new String(randomBytes, StandardCharsets.UTF_8);

		assertThat(NamingConvention.NULL.convert(randomString)).isEqualTo(randomString);
	}
}
