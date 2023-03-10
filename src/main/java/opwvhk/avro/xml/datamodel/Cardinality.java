package opwvhk.avro.xml.datamodel;

import java.util.Optional;

import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaParticle;
import org.apache.ws.commons.schema.XmlSchemaUse;

import static org.apache.ws.commons.schema.XmlSchemaUse.NONE;

/**
 * <p>Field cardinality: describes how many field values can be present in an object.</p>
 *
 * <p>The constants are in order, from most to least restrictive. The method {@link #adjustFor(Cardinality)} requires this order.</p>
 */
public enum Cardinality {
	/**
	 * 1..1 occurrences.
	 */
	REQUIRED() {
		@Override
		public String formatName(String name) {
			return name;
		}
	},
	/**
	 * 0..1 occurrences.
	 */
	OPTIONAL() {
		@Override
		public String formatName(String name) {
			return name + "?";
		}
	},
	/**
	 * 0..n occurrences.
	 */
	MULTIPLE() {
		@Override
		public String formatName(String name) {
			return name + "[]";
		}
	};

	/**
	 * Default cardinality to use.
	 */
	public static final Cardinality DEFAULT_VALUE = REQUIRED;

	/**
	 * Determine the cardinality of an XML attribute.
	 *
	 * @param attribute an XML attribute
	 * @return the cardinality of the attribute
	 */
	public static Cardinality of(XmlSchemaAttribute attribute) {
		XmlSchemaUse use = Optional.ofNullable(attribute.getUse()).orElse(NONE);
		return switch (use) {
			case REQUIRED -> REQUIRED;
			case NONE, OPTIONAL -> OPTIONAL;
			default -> throw new IllegalArgumentException("Attribute use=" + use + " is not supported");
		};
	}

	/**
	 * Determine the cardinality of an XML particle (element or group of elements).
	 *
	 * @param particle an XML particle
	 * @return the cardinality of the particle
	 */
	public static Cardinality of(XmlSchemaParticle particle) {
		if (particle.getMaxOccurs() > 1) {
			return MULTIPLE;
		} else if (particle.getMinOccurs() < 1 || (particle instanceof XmlSchemaElement element && element.isNillable())) {
			return OPTIONAL;
		} else {
			return REQUIRED;
		}
	}

	/**
	 * Adjust cardinality for child particles. Returns the most permissive cardinality of this and the other cardinality.
	 *
	 * @param childCardinality the cardinality defined for child particles
	 * @return the cardinality to use for the particle
	 */
	public Cardinality adjustFor(Cardinality childCardinality) {
		return ordinal() > childCardinality.ordinal() ? this : childCardinality;
	}

	/**
	 * <p>Format a name according to this cardinality.</p>
	 *
	 * <p>This yields the name, possibly followed by {@code ?} or {@code []} (for optional and multiple elements, respectively).</p>
	 *
	 * @param name a name to format
	 * @return the name, with possibly a suffix to describe cardinality
	 */
	public abstract String formatName(String name);
}
