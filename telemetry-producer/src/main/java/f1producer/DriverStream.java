/**
 * Interface for reading driver telemetry streams.
 * 
 * What it does:
 * Defines a standard contract (hasNext, peek, poll, close) for streaming telemetry events.
 * 
 * Why it exists:
 * Applies the Dependency Inversion Principle, allowing the MergeCoordinator to work with any stream 
 * of data (e.g., CSV, API, Database) without being tightly coupled to file I/O logic.
 */
package f1producer;

public interface DriverStream {

    boolean hasNext();

    TelemetryRecord poll() throws Exception;
}