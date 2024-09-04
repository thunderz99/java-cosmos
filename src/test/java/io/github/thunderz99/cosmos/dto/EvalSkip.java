package io.github.thunderz99.cosmos.dto;

public class EvalSkip extends EvalBase {

    private EvalSkip() {
    }

    public static final EvalSkip singleton = new EvalSkip();

    @Override
    public boolean equals(Object that) {
        return that instanceof EvalSkip;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "skip";
    }

}
