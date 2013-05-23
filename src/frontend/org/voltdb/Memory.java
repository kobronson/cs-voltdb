package org.voltdb;
import com.sun.management.OperatingSystemMXBean;
/**
 * Information of used memory
 *
 * @author Kamil Szmit
 */
public abstract class Memory
{
    private static float limitUsagePercentage = 100;
    private static float percentageOfDataToMove = 0;
    private static boolean coldStorageIsEnabled = false;
    /**
     * Gets the percentage of used random access memory
     *
     * @return the number of percentages of used RAM
     */
    public static double getUsage()
    {
        OperatingSystemMXBean os = (OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        double total = os.getTotalPhysicalMemorySize();
        return 100 * (total - (double) os.getFreePhysicalMemorySize()) / total;
    }
    /**
     * Checks if the used memory reached the limit
     *
     * @return true if the memory used is equal to or greater than the limit
     */
    public static boolean shouldBeMoveOnDisk()
    {
        
        return (limitUsagePercentage <= getUsage());
    }
    /**
     * Sets the limit percentage of used RAM
     *
     * @param limit the number of percentages
     */
    public static void setLimitUsagePercentage(final float limit)
    {
        limitUsagePercentage = limit;
    }
    /**
     * Gets the percentage of data to move on disk when the memory used is equal to or greater than the limit
     *
     * @return the number of percentages of data
     */
    public static float getPercentageOfDataToMove()
    {
        return percentageOfDataToMove;
    }
    /**
     * Sets the percentage of data to move on disk when the memory used is equal to or greater than the limit
     *
     * @param percentage the number of percentages of data
     */
    public static void setPercentageOfDataToMove(final float percentage)
    {
        percentageOfDataToMove = percentage;
    }
    /**
     * Checks if cold storage is enabled
     *
     * @return true if cold storage is enabled
     */
    public static boolean coldStorageIsEnabled()
    {
        return coldStorageIsEnabled;
    }
    /**
     * Sets cold storage enabled or disabled
     *
     * @param isEnabled true if cold storage is enabled
     */
    public static void setColdStorageEnabled(final boolean isEnabled)
    {
        coldStorageIsEnabled = isEnabled;
    }
}
