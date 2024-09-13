package io.github.thunderz99.cosmos.condition;

/**
 * sub query 's OR / AND / NOT operator
 */
public class SubConditionType {

    public static final String AND = "$AND";
    public static final String OR = "$OR";
    public static final String NOT = "$NOT";

    /**
     * only support mongodb at present
     *
     * <pre>
     * db.Families.find({
     *   "children": {
     *     "$elemMatch": {
     *       "age": { "$gt": 10 },
     *       "sex": { "$eq": "female" }
     *     }
     *   }
     * })
     *
     * </pre>
     */
    public static final String ELEM_MATCH = "$ELEM_MATCH";

}
