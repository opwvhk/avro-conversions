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

package opwvhk.avro.json;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.net.URI;

/**
 * Runtime exception for analysis failures. Used to indicate a failure in analysing a data schema.
 * {@link opwvhk.avro.json.JsonAsAvroParser#JsonAsAvroParser(URI, boolean, Schema, GenericData )}
 */
public class AnalysisFailure extends RuntimeException {
	/**
	 * Create an analysis failure for the specified reason and underlying cause.
	 *
	 * @param message describing the reason analysis failed
	 * @param cause   the underlying cause for the failure
	 */
	public AnalysisFailure(String message, Throwable cause) {
		super(message, cause);
	}
}
