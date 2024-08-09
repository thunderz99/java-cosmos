package io.github.thunderz99.cosmos.dto;

import java.util.List;

import com.google.common.collect.Lists;

/**
 * DTO class representing a UniqueKeyPolicy containing multiple uniqueKeys for a Cosmos Container.
 */
public class UniqueKeyPolicy extends RecordData {
    public List<UniqueKey> uniqueKeys = Lists.newArrayList();
}
