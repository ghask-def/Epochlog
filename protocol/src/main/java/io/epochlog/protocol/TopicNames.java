package io.epochlog.protocol;

import java.util.regex.Pattern;

/**
 * Topic name rules shared by brokers and the controller.
 *
 * <p>Thread-safety: thread-safe (stateless).
 */
public final class TopicNames {
    private static final Pattern LEGAL = Pattern.compile("[a-zA-Z0-9._-]{1,200}");

    private TopicNames() {}

    /** Returns null if the name is fine, otherwise why it isn't. */
    public static String validate(String name, boolean allowInternal) {
        if (name == null || !LEGAL.matcher(name).matches()) {
            return "topic names must match " + LEGAL.pattern();
        }
        if (name.startsWith("__") && !allowInternal) {
            return "names starting with __ are reserved for internal topics";
        }
        return null;
    }
}
