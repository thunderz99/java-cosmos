package io.github.thunderz99.cosmos.condition;

import java.util.concurrent.atomic.AtomicInteger;

import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;

public interface Expression {

    public CosmosSqlQuerySpec toQuerySpec(AtomicInteger paramIndex, String selectAlias);

}
