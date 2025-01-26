package io.github.thunderz99.cosmos.impl.postgres.dto;

import java.util.LinkedHashSet;
import java.util.Set;

import io.github.thunderz99.cosmos.dto.RecordData;

/**
 * Filter options for postgres
 */
public class PGFilterOptions extends RecordData {

    /**
     * If we have a join
     */
    public Set<String> join = new LinkedHashSet<>();

    /**
     * If we are in an inner condition for a json path expression
     */
    public boolean innerCond = false;


    /**
     * Factory method
     * @return new FilterOptions
     */
    public static PGFilterOptions create(){
        return new PGFilterOptions();
    }

    /**
     * Set the join option
     * @param join
     * @return this
     */
    public PGFilterOptions join(Set<String> join){
        this.join = join;
        return this;
    }

    /**
     * Set the innerCond option
     * @param innerCond
     * @return this
     */
    public PGFilterOptions innerCond(boolean innerCond){
        this.innerCond = innerCond;
        return this;
    }

}
