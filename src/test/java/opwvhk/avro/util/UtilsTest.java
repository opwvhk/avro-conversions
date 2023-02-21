package opwvhk.avro.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Base64;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.assertj.core.api.Assertions.*;

public class UtilsTest {

	@Test
	public void digest() {
		assertThat(Base64.getEncoder().encodeToString(Utils.digest("md5").digest(new byte[0]))).isEqualTo("1B2M2Y8AsgTpgAmY7PhCfg==");
		assertThatThrownBy(() -> Utils.digest("foobar")).isInstanceOf(IllegalArgumentException.class).hasMessage("Unknown algorithm");
	}


	@Test
	public void snakeCaseCanBeConvertedToCamelCase() {
		assertThat(Utils.snakeToUpperCamelCase("MVP_description")).isEqualTo("MVPDescription");
		assertThat(Utils.snakeToUpperCamelCase("URL_list")).isEqualTo("URLList");
		assertThat(Utils.snakeToUpperCamelCase("URLs")).isEqualTo("URLs");
		assertThat(Utils.snakeToUpperCamelCase("simple_name")).isEqualTo("SimpleName");

		assertThat(Utils.snakeToLowerCamelCase("MVP_description")).isEqualTo("mvpDescription");
		assertThat(Utils.snakeToLowerCamelCase("URL_list")).isEqualTo("urlList");
		assertThat(Utils.snakeToLowerCamelCase("URLs")).isEqualTo("urls");
		assertThat(Utils.snakeToLowerCamelCase("simple_name")).isEqualTo("simpleName");
	}

	@Test
	public void findFirstNonNullParameter() {
		assertThat(Utils.first(1, null, null)).isEqualTo(1);
		assertThat(Utils.first(null, 2, null)).isEqualTo(2);
		assertThat(Utils.first(null, null, 3)).isEqualTo(3);

		assertThat(Utils.<String>first(null, null, null)).isNull();
		assertThat(Utils.<String>first()).isNull();
	}

	@Test
	public void truncatingText() {
		assertThat(Utils.truncate(10, null)).isNull();

		assertThat(Utils.truncate(30, "This is a simple sentence.")).isEqualTo("This is a simple sentence.");
		assertThat(Utils.truncate(20, "This is a simple sentence.")).isEqualTo("This is a simple se…");
		assertThat(Utils.truncate(10, "This is a simple sentence.")).isEqualTo("This is a…");
	}
}
