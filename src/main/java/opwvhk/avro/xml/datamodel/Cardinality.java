package opwvhk.avro.xml.datamodel;

import java.util.Optional;

import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaParticle;
import org.apache.ws.commons.schema.XmlSchemaUse;

import static org.apache.ws.commons.schema.XmlSchemaUse.NONE;

public enum Cardinality {
	// Possible field occurrences, in order of most to least restrictive (the method adjustFor() required this order!)
	REQUIRED() {
		@Override
		public String formatName(String name) {
			return name;
		}
	}, OPTIONAL() {
		@Override
		public String formatName(String name) {
			return name + "?";
		}
	}, MULTIPLE() {
		@Override
		public String formatName(String name) {
			return name + "[]";
		}
	};

	public static Cardinality defaultValue() {
		return REQUIRED;
	}

	public static Cardinality of(XmlSchemaAttribute attribute) {
		XmlSchemaUse use = Optional.ofNullable(attribute.getUse()).orElse(NONE);
		return switch (use) {
			case REQUIRED -> REQUIRED;
			case NONE, OPTIONAL -> OPTIONAL;
			default -> throw new IllegalArgumentException("Attribute use=" + use + " is not supported");
		};
	}

	public static Cardinality of(XmlSchemaParticle particle) {
		if (particle.getMaxOccurs() > 1) {
			return MULTIPLE;
		} else if (particle.getMinOccurs() < 1 || (particle instanceof XmlSchemaElement element && element.isNillable())) {
			return OPTIONAL;
		} else {
			return REQUIRED;
		}
	}

	public Cardinality adjustFor(Cardinality childCardinality) {
		return ordinal() > childCardinality.ordinal() ? this : childCardinality;
	}

	public abstract String formatName(String name);
}
