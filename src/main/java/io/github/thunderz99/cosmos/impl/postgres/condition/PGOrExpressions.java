package io.github.thunderz99.cosmos.impl.postgres.condition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.github.thunderz99.cosmos.condition.Expression;
import io.github.thunderz99.cosmos.condition.SimpleExpression;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;

/**
 * Expressions like "firstName OR lastName STARTSWITH" : "H" for postgres
 *
 * @author zhang.lei
 *
 */
public class PGOrExpressions implements Expression {

	public List<PGSimpleExpression> simpleExps = new ArrayList<>();

	public PGOrExpressions() {
	}

	public PGOrExpressions(List<PGSimpleExpression> simpleExps, Object value) {
		this.simpleExps = simpleExps;
	}

    public PGOrExpressions(String key, Object value) {
        var keys = key.split(" OR ");

        if (keys == null || keys.length == 0) {
            return;
        }
        this.simpleExps = List.of(keys).stream().map(k -> new PGSimpleExpression(k, value)).collect(Collectors.toList());
    }

    public PGOrExpressions(String key, Object value, String operator) {
        var keys = key.split(" OR ");

        if (keys == null || keys.length == 0) {
            return;
        }
        this.simpleExps = List.of(keys).stream().map(k -> new PGSimpleExpression(k, value, operator))
                .collect(Collectors.toList());
    }

    @Override
    public CosmosSqlQuerySpec toQuerySpec(AtomicInteger paramIndex, String selectAlias) {

        var ret = new CosmosSqlQuerySpec();

        if (simpleExps == null || simpleExps.isEmpty()) {
            return ret;
        }

        var indexForQuery = paramIndex;
        var indexForParam = new AtomicInteger(paramIndex.get());

        var queryText = simpleExps.stream().map(exp -> exp.toQuerySpec(indexForQuery, selectAlias).getQueryText())
                .collect(Collectors.joining(" OR", " (", " )"));

        var params = simpleExps.stream().map(exp -> exp.toQuerySpec(indexForParam, selectAlias).getParameters())
                .reduce(new ArrayList<>(), (sum, elm) -> {
                    sum.addAll(elm);
                    return sum;
                });
        ;

        ret.setQueryText(queryText);
        ret.setParameters(params);

        return ret;

    }

}