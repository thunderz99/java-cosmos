package io.github.thunderz99.cosmos.condition;

import java.util.concurrent.atomic.AtomicInteger;

import com.microsoft.azure.documentdb.SqlQuerySpec;

public interface Expression {

	public SqlQuerySpec toQuerySpec(AtomicInteger paramIndex);

}
