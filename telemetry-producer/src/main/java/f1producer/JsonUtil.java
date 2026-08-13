/**
 * Utility class for JSON serialization.
 * 
 * What it does:
 * Wraps the Jackson ObjectMapper to convert TelemetryRecord objects into JSON strings.
 * 
 * Why it exists:
 * Centralizes the JSON configuration (such as allowing unquoted field names) to ensure 
 * consistent formatting of the payload sent to Kafka.
 */
package f1producer;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

    private static final ObjectMapper mapper = new ObjectMapper()
        .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature());

    public static String toJson(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}