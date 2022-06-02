package com.gantrol.extsort;

public class Utils {

    public static String usage() {
        return "java -jar extsort inputfile outputfile";
    }

    /**
     * 调用gc，并计算可用内存
     *
     * reference: http://stackoverflow.com/questions/12807797/java-get-available-memory
     *
     * @return available memory
     */
    public static long estimateAvailableMemory() {
        System.gc();
        Runtime r = Runtime.getRuntime();
        long allocatedMemory = r.totalMemory() - r.freeMemory();
        return r.maxMemory() - allocatedMemory;
    }

    /**
     * we divide the file into small blocks. If the blocks are too small, we
     * shall create too many temporary files. If they are too big, we shall
     * be using too much memory.
     *
     * @param sizeoffile  how much data (in bytes) can we expect
     * @param maxtmpfiles how many temporary files can we create (e.g., 1024)
     * @param maxMemory   Maximum memory to use (in bytes)
     * @return the estimate
     */
    public static long estimateBestSizeOfBlocks(final long sizeoffile,
                                                final int maxtmpfiles, final long maxMemory) {
        // we don't want to open up much more than maxtmpfiles temporary
        // files, better run
        // out of memory first.
        long blocksize = sizeoffile / maxtmpfiles
                + (sizeoffile % maxtmpfiles == 0 ? 0 : 1);

        // on the other hand, we don't want to create many temporary
        // files
        // for naught. If blocksize is smaller than half the free
        // memory, grow it.
        if (blocksize < maxMemory / 2) {
            blocksize = maxMemory / 2;
        }
        return blocksize;
    }
}
