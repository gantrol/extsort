package com.gantrol.extsort;


import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class ExternalSort {

    public static int DEFAULT_MAX_TEMP_FILES = 1024;
    final int BUFFER_SIZE = 2048;
    // TODO: 4学明白这个……
    public Comparator<Integer> defaultComparator = Comparator.comparingInt(o -> o);
    // TODO: 3初始化的时候提供选项……
    public int maxTempFiles = DEFAULT_MAX_TEMP_FILES;
    public Comparator<Integer> cmp = defaultComparator;
    boolean useGzip = true;
    Charset cs = StandardCharsets.UTF_8;


    /**
     * @param args command line argument
     * @throws IOException generic IO exception
     */
    public static void main(final String[] args) throws IOException {

        if (args.length < 2) {
            System.out.println(Utils.usage());
        } else {
            File tempFileStore = new File("tmp/");

            ExternalSort externalSort = new ExternalSort();

            String inputFile = args[0];
            String outputFile = args[1];

            List<File> l = externalSort.sortInBatch(new File(inputFile), tempFileStore);
            long linesOfNumbers = externalSort.mergeSortedFiles(l, new File(outputFile), false);
            System.out.println("为" + linesOfNumbers + "行排序");
        }
    }

    public List<File> sortInBatch(File file, File tmpDir)
            throws IOException {

        long dataLength = file.length();
        long availableMemory = Utils.estimateAvailableMemory();

        List<File> files = new ArrayList<>();
        long blockSize = Utils.estimateBestSizeOfBlocks(dataLength, maxTempFiles, availableMemory);

        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(file))) {
            List<Integer> tmpList = new ArrayList<>();
            try {
                long curBlockSize = 0; // in bytes
                while (dataInputStream.available() > 0) {
                    while (curBlockSize < blockSize) {
                        int i = dataInputStream.readInt();
                        tmpList.add(i);
                        // TODO: 1 int多大合适？据说是4bit，那我直接四倍应该够了？
                        curBlockSize += 16;
                    }
                    sortAndSave(files, tmpList, tmpDir);
                }
            } catch (EOFException oef) {
                if (tmpList.size() > 0) {
                    sortAndSave(files, tmpList, tmpDir);
                }
            }
        }
        return files;
    }

    void sortAndSave(List<File> files, List<Integer> ints, File tmpdirectory) throws IOException {
        files.add(save(sort(ints), tmpdirectory));
        ints.clear();
    }

    List<Integer> sort(List<Integer> ints) {
        return ints.parallelStream().sorted(cmp).collect(Collectors.toList());
    }

    File save(List<Integer> ints, File tmpdirectory) throws IOException {
        File newtmpfile = File.createTempFile("sort", ".tmp", tmpdirectory);
        newtmpfile.deleteOnExit();
        OutputStream out = new FileOutputStream(newtmpfile);
        if (useGzip) {
            out = new GZIPOutputStream(out, BUFFER_SIZE) {
                {
                    this.def.setLevel(Deflater.BEST_SPEED);
                }
            };
        }
        try (BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(
                out, cs))) {
            Iterator<Integer> iterator = ints.listIterator();
            if (iterator.hasNext()) {
                fbw.write(String.valueOf(iterator.next()));
                while (iterator.hasNext()) {
                    fbw.newLine();
                    fbw.write(String.valueOf(iterator.next()));
                }
            }
        }
        return newtmpfile;
    }

    public long mergeSortedFiles(List<File> files, File outputfile, boolean append) throws IOException {

        ArrayList<StreamStack> bfbs = new ArrayList<>();
        for (File f : files) {
            if (f.length() == 0) {
                continue;
            }
            InputStream in = new FileInputStream(f);
            BufferedReader br;
            if (useGzip) {
                br = new BufferedReader(
                        new InputStreamReader(
                                new GZIPInputStream(in, BUFFER_SIZE),
                                cs));
            } else {
                br = new BufferedReader(new InputStreamReader(in, cs));
            }

            BinaryFileBuffer bfb = new BinaryFileBuffer(br);
            bfbs.add(bfb);
        }
        BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(outputfile, append), cs));
        long rowcounter = priority(fbw, bfbs);
        for (File f : files) {
            f.delete();
        }
        return rowcounter;
    }

    public long priority(BufferedWriter fbw, List<StreamStack> buffers) throws IOException {
        PriorityQueue<StreamStack> pq = new PriorityQueue<>(
                11, (i, j) -> cmp.compare(i.peek(), j.peek()));
        for (StreamStack bfb : buffers) {
            if (!bfb.empty()) {
                pq.add(bfb);
            }
        }
        long rowcounter = 0;
        try (fbw) {
            while (pq.size() > 0) {
                StreamStack bfb = pq.poll();
                int r = bfb.pop();
                fbw.write(r);
                fbw.newLine();
                ++rowcounter;
                if (bfb.empty()) {
                    bfb.close();
                } else {
                    pq.add(bfb); // add it back
                }
            }

        } finally {
            for (StreamStack bfb : pq) {
                bfb.close();
            }
        }
        return rowcounter;

    }
}
