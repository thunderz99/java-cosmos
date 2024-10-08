package io.github.thunderz99.cosmos.condition;

/**
 * sub query 's OR / AND / NOT operator
 */
public class SubConditionType {

    public static final String AND = "$AND";
    public static final String OR = "$OR";
    public static final String NOT = "$NOT";

    /**
     * support a simple expression for query
     *
     * <p>
     * {@code
     * // e.g.
     * Condition.filter("$EXPRESSION", "c.age / 10 < ARRAY_LENGTH(c.skills)");
     * }
     * </p>
     */
    public static final String EXPRESSION = "$EXPRESSION";

    /**
     * <p>find only the array element that fullfil all the sub conditions</p>
     * <p>only support mongodb at present</p>
     *
     * <pre>
     * db.Families.find({
     *   "children": {
     *     "$elemMatch": {
     *       "grade": { "$gt": 5 },
     *       "gender": { "$eq": "female" }
     *     }
     *   }
     * })
     *
     * </pre>
     */
    public static final String ELEM_MATCH = "$ELEM_MATCH";


}
