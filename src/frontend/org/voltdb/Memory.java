package org.voltdb;

/**
 * Information of used memory
 *
 * @author Kamil Szmit
 */
public class Memory
{
    /**
     * Get the percentage of used random access memory
     *
     * @return the number of percentages of used RAM
     */
    public double GetUsage()
    {
        Runtime runtime = Runtime.getRuntime();
        double total = runtime.totalMemory();
        return (total - runtime.freeMemory()) / total;
    }
}
