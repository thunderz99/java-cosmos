package io.github.thunderz99.cosmos.dto;

/**
 * Options when using partial update methods. e.g. whether check etags to implement a
 */
public class PartialUpdateOption {

    public boolean checkETag = false;

    public static PartialUpdateOption checkETag(boolean checkETag){
        var option = new PartialUpdateOption();
        option.checkETag = checkETag;
        return option;
    }

}
