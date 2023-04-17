package opwvhk.avro.json;

import java.io.IOException;

interface JsonHandler {
    void startObject();

    void startField() throws IOException;

    void endObject() throws IOException;

    void startArray();

    void endArray() throws IOException;

    void scalarValue(String value) throws IOException;
}
