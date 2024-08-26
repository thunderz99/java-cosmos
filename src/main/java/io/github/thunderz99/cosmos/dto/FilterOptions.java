package io.github.thunderz99.cosmos.dto;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Filter options for mongodb
 */
public class FilterOptions extends RecordData{

    /**
     * If we have a join
     */
    public Set<String> join = new LinkedHashSet<>();

    /**
     * If we are in an inner condition for an aggregate pipeline's $filter / cond
     */
    public boolean innerCond = false;


    /**
     * Factory method
     * @return new FilterOptions
     */
    public static FilterOptions create(){
        return new FilterOptions();
    }

    /**
     * Set the join option
     * @param join
     * @return this
     */
    public FilterOptions join(Set<String> join){
        this.join = join;
        return this;
    }

    /**
     * Set the innerCond option
     * @param innerCond
     * @return this
     */
    public FilterOptions innerCond(boolean innerCond){
        this.innerCond = innerCond;
        return this;
    }

}
