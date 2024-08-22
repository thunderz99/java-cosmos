package io.github.thunderz99.cosmos.v4;

import java.util.List;

import com.azure.cosmos.implementation.patch.PatchOperation;
import com.azure.cosmos.implementation.patch.PatchOperationCore;
import com.azure.cosmos.implementation.patch.PatchOperationType;
import com.azure.cosmos.models.CosmosPatchOperations;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A bean class representing a serials of patch operations
 *
 * <p>
 * Compared to the official CosmosPatchOperations, this class offers smarter error check and getSize methods.
 * </p>
 * <p>
 * For more details, see <a href="https://docs.microsoft.com/en-us/azure/cosmos-db/partial-document-update-getting-started?tabs=java">CosmosPatchOperations official doc</a>
 * </p>
 */
public class PatchOperations {

    /**
     * Official Cosmos DB's PatchOperations' limit to be processed in one call.
     */
    public static int LIMIT = 10;

    /**
     * Inner cosmos patch operations
     */
    CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();

    /**
     * Operations to record PatchOperation and size
     */
    List<PatchOperation> operations = Lists.newArrayList();

    protected PatchOperations() {

    }

    /**
     * Create an empty PatchOperations instance
     *
     * @return empty PatchOperations instance
     */
    public static PatchOperations create(){
        return new PatchOperations();
    }

    /**
     * Add an "add operation"
     *
     * @param path json path
     * @param value value to add
     * @param <T> type of value
     * @return PatchOperations it self
     */
    public <T> PatchOperations add(String path, T value) {
        this.operations.add(new PatchOperationCore(PatchOperationType.ADD, path, value));
        this.cosmosPatchOperations.add(path, value);
        checkPath(path);
        return this;
    }

    /**
     * Check if path starts with "/"
     *
     * @param path
     */
    static void checkPath(String path) {
        Preconditions.checkArgument(path.startsWith("/"), "Json path(%s) must start with /", path);
    }

    /**
     * Add an "remove operation"
     *
     * @param path json path
     * @return PatchOperations it self
     */

    public PatchOperations remove(String path) {
        this.operations.add(new PatchOperationCore(PatchOperationType.REMOVE, path, (Object)null));
        this.cosmosPatchOperations.remove(path);
        checkPath(path);
        return this;
    }

    /**
     * Add an "replace operation"
     *
     * @param path json path
     * @param value value to replace
     * @param <T> type of value
     * @return PatchOperations it self
     */

    public <T> PatchOperations replace(String path, T value) {
        this.operations.add(new PatchOperationCore(PatchOperationType.REPLACE, path, value));
        this.cosmosPatchOperations.replace(path, value);
        checkPath(path);
        return this;
    }

    /**
     * Add an "set operation"
     *
     * @param path json path
     * @param value value to set
     * @param <T> type of value
     * @return PatchOperations it self
     */

    public <T> PatchOperations set(String path, T value) {
        this.operations.add(new PatchOperationCore(PatchOperationType.SET, path, value));
        this.cosmosPatchOperations.set(path, value);
        checkPath(path);
        return this;
    }

    /**
     * Add an "increment operation" of long
     *
     * @param path json path
     * @param value value to add
     * @return PatchOperations it self
     */

    public PatchOperations increment(String path, long value) {
        this.operations.add(new PatchOperationCore(PatchOperationType.INCREMENT, path, value));
        this.cosmosPatchOperations.increment(path, value);
        checkPath(path);
        return this;
    }

    /**
     * Add an "increment operation" of double
     *
     * @param path json path
     * @param value value to add
     * @return PatchOperations it self
     */

    public PatchOperations increment(String path, double value) {
        this.operations.add(new PatchOperationCore(PatchOperationType.INCREMENT, path, value));
        this.cosmosPatchOperations.increment(path, value);
        checkPath(path);
        return this;
    }

    /**
     * Get patchOperations' size
     *
     * @return size of patch operations
     */
    public int size() {
        return operations.size();
    }

    /**
     * Get the cosmosPatchOperations
     *
     * @return cosmosPatchOperations
     */
    public CosmosPatchOperations getCosmosPatchOperations() {
        return this.cosmosPatchOperations;
    }

    /**
     * Get the list of operations
     *
     * @return operations list
     */
    public List<PatchOperation> getPatchOperations() {
        return this.operations;
    }
}
