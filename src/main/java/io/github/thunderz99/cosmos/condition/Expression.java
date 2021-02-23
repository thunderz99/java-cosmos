package io.github.thunderz99.cosmos.condition;

import com.azure.cosmos.models.SqlQuerySpec;

import java.util.concurrent.atomic.AtomicInteger;

public interface Expression {

	public SqlQuerySpec toQuerySpec(AtomicInteger paramIndex);

}
