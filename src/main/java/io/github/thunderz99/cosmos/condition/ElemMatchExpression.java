package io.github.thunderz99.cosmos.condition;

import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ElemMatchExpression implements Expression {

    private final Map<String, Object> subFilters;
    private final Condition condition;

    public ElemMatchExpression(Map<String, Object> subFilters, Condition condition) {
        this.subFilters = subFilters;
        this.condition = condition;
    }

    @Override
    public CosmosSqlQuerySpec toQuerySpec(AtomicInteger paramIndex, String selectAlias) {
        String arrayField = subFilters.keySet().stream().findFirst()
                .map(k -> StringUtils.substringBefore(k, ".")).orElse("");

        if (StringUtils.isEmpty(arrayField)) {
            throw new IllegalArgumentException("$ELEM_MATCH requires a field path like 'arrayField.subField'");
        }

        var params = new ArrayList<CosmosSqlParameter>();

        List<String> subCondTexts = new ArrayList<>();

        for (var subEntry : subFilters.entrySet()) {
            String key = subEntry.getKey();
            if (!key.startsWith(arrayField + ".")) {
                throw new IllegalArgumentException(String.format(
                        "Inconsistent array field in $ELEM_MATCH. Expected prefix '%s' but found key '%s'",
                        arrayField, key));
            }
            var exp = Condition.parse(key, subEntry.getValue());
            var spec = exp.toQuerySpec(paramIndex, selectAlias);
            params.addAll(spec.getParameters());
            subCondTexts.add(spec.getQueryText());
        }

        String combinedCondText = String.join(" AND ", subCondTexts);
        condition.saveOriginJoinCondition(combinedCondText);

        String queryText = condition.toJoinQueryText(arrayField, combinedCondText, paramIndex);
        return new CosmosSqlQuerySpec(queryText, params);
    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }
}