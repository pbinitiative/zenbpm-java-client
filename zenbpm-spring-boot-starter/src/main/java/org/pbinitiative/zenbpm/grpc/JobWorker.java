package org.pbinitiative.zenbpm.grpc;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a ZenBPM job worker for the given job type.
 *
 * <p>Example usage:
 * <pre>
 *   @JobWorker("payment")
 *   public void handlePayment(WaitingJob job) {
 *       // ...
 *   }
 * </pre>
 *
 * The annotation is retained at runtime so that Spring components in this starter
 * can discover and register workers automatically in future enhancements.
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface JobWorker {
    /**
     * The job type to subscribe to and handle.
     */
    String value();
}
