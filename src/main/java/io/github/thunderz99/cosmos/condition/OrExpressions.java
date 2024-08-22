package io.github.thunderz99.cosmos.condition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;

/**
 * Expressions like "firstName OR lastName STARTSWITH" : "H"
 *
 * @author zhang.lei
 *
 */
public class OrExpressions implements Expression {

	public List<SimpleExpression> simpleExps = new ArrayList<>();

	public OrExpressions() {
	}

	public OrExpressions(List<SimpleExpression> simpleExps, Object value) {
		this.simpleExps = simpleExps;
	}

    public OrExpressions(String key, Object value) {
        var keys = key.split(" OR ");

        if (keys == null || keys.length == 0) {
            return;
        }
        this.simpleExps = List.of(keys).stream().map(k -> new SimpleExpression(k, value)).collect(Collectors.toList());
    }

    public OrExpressions(String key, Object value, String operator) {
        var keys = key.split(" OR ");

        if (keys == null || keys.length == 0) {
            return;
        }
        this.simpleExps = List.of(keys).stream().map(k -> new SimpleExpression(k, value, operator))
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