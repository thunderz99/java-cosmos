package io.github.thunderz99.cosmos.condition;

import io.github.thunderz99.cosmos.dto.RecordData;

/**
 * A class representing a key of a json field. e.g. "name" for "c.name" or "address.country" for "c.address.country"
 *
 * <pre>
 *     e.g. find documents where mail field does not equal mail2 field
 *     Condition.filter("mail !=", Condition.key("mail2"))
 * </pre>
 */
public class FieldKey extends RecordData {
    public String keyName;

    public FieldKey(){
    }

    public FieldKey(String keyName){
        this.keyName = keyName;
    }

}
