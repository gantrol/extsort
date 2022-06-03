package com.gantrol.extsort;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.*;


public class ExternalSortTest {

    private static final String TEST_FILE1_TXT = "test-file-1.txt";
    private static final String TEST_FILE1_OUTPUT_TXT = "test-file-1_sorted.txt";
    private static final String TEST_FILE2_TXT = "test-file-2.txt";
    private static final String TEST_FILE1_2_OUTPUT_TXT = "test-file1-2_sorted.txt";
    private static final String TEST_FILE1M_TXT = "input.txt";

    private static final Integer[] EXPECTED_SORT_RESULTS = {
            -2147483648, -954384567, -15684528, -8745645, -50,
            0,
            23490, 23490, 843234, 3456362, 424523454, 2147483647
    };

    private static final Integer[] SAMPLE = {
            424523454, 2147483647, -2147483648, -15684528, -8745645,
            23490,
            23490, 3456362, 0, 843234, -50, -954384567
    };

    private File file1;
    private File file2;
    private File file12;
    private File file1M;
    private List<File> fileList;

    private static void copyFile(File sourceFile, File destFile) throws IOException {
        if (!destFile.exists()) {
            destFile.createNewFile();
        }

        try (FileInputStream fis = new FileInputStream(sourceFile);
             FileChannel source = fis.getChannel();
             FileOutputStream fos = new FileOutputStream(destFile);
             FileChannel destination = fos.getChannel()) {
            destination.transferFrom(source, 0, source.size());
        }

    }

    @Before
    public void setUp() throws Exception {
        this.fileList = new ArrayList<>(2);
        this.file1 = new File(this.getClass().getClassLoader().getResource(TEST_FILE1_TXT).toURI());
        this.file2 = new File(this.getClass().getClassLoader().getResource(TEST_FILE2_TXT).toURI());
        this.file12 = new File(this.getClass().getClassLoader().getResource(TEST_FILE1_2_OUTPUT_TXT).toURI());
        this.file1M = new File(this.getClass().getClassLoader().getResource(TEST_FILE1M_TXT).toURI());

        File tmpFile1 = new File(this.file1.getPath() + ".tmp");
        File tmpFile2 = new File(this.file2.getPath() + ".tmp");

        copyFile(this.file1, tmpFile1);
        copyFile(this.file2, tmpFile2);
        this.fileList.add(tmpFile1);
        this.fileList.add(tmpFile2);
    }

    @After
    public void tearDown() {
        this.file1 = null;
        this.file2 = null;
        for (File f : this.fileList) {
            f.delete();
        }
        this.fileList.clear();
        this.fileList = null;
    }

    // TODO: 1 test integer estimation

    @Test
    public void displayTest() throws Exception {
        ExternalSort.main(new String[]{}); // check that it does not crash
    }

    @Test
    public void testEmptyFiles() throws Exception {
        File f1 = File.createTempFile("tmp", "unit");
        File f2 = File.createTempFile("tmp", "unit");
        f1.deleteOnExit();
        f2.deleteOnExit();
        ExternalSort externalSort = new ExternalSort();
        externalSort.mergeSortedFiles(externalSort.sortInBatch(f1, new File("./")), f2, false);
        if (f2.length() != 0) throw new RuntimeException("empty files should end up emtpy");
    }

    @Test
    public void testSortAndSave() throws Exception {
        // TODO: 3 test not gzip, 参数化
        File f;
        String line;

        List<Integer> sample = Arrays.asList(SAMPLE);
        ExternalSort externalSort = new ExternalSort();
        f = externalSort.save(externalSort.sort(sample), null);
        assertNotNull(f);
        assertTrue(f.exists());
        assertTrue(f.length() > 0);
        List<Integer> result = new ArrayList<>();
        try (BufferedReader scanner = new BufferedReader(
                new InputStreamReader(
                        new GZIPInputStream(new FileInputStream(f), externalSort.BUFFER_SIZE), externalSort.cs))) {
            while ((line = scanner.readLine()) != null) {
                int num = Integer.parseInt(line);
                result.add(num);
            }
        }
        assertArrayEquals(Arrays.toString(result.toArray()), EXPECTED_SORT_RESULTS, result.toArray());
    }

    @Test
    public void testSortFile1() throws IOException {

        File tmpFile1 = fileList.get(0);
        ExternalSort externalSort = new ExternalSort();
        File output = new File(TEST_FILE1_OUTPUT_TXT);
        externalSort.externalSort(tmpFile1, null, output);
        // TODO: check lines...
        // test file content sorted
        assertTrue(isFileSorted(output));
        output.delete();

    }

    private boolean isFileSorted(File file) throws IOException {
        // TODO: read and keep pre int
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = br.readLine();
        if (line != null) {
            int pre = Integer.parseInt(line);
            while ((line = br.readLine()) != null) {
                int cur = Integer.parseInt(line);
                if (pre > cur) {
                    return false;
                }
            }
        }
        return true;
    }

    @Test
    public void testMergeSortedFile() throws IOException {
        ExternalSort externalSort = new ExternalSort();
        externalSort.useGzip = false;
        File sortedFile1 = new File("file1.sorted.txt");
        File sortedFile2 = new File("file2.sorted.txt");
        externalSort.externalSort(fileList.get(0), null, sortedFile1);
        externalSort.externalSort(fileList.get(1), null, sortedFile2);
        List<File> files = new ArrayList<>();
        files.add(sortedFile1);
        files.add(sortedFile2);
        externalSort.mergeSortedFiles(files, file12, false);
        assertTrue(isFileSorted(file12));
    }

//    // TODO: large file and test result order
    @Test
    public void testMain() throws Exception {
        String s = file1M.toPath().toAbsolutePath().toString();
        ExternalSort.main(new String[]{s, "output.txt"});
    }

//    @Test
//    public void testMain1G() throws Exception {
//        String s = "C:\\Users\\50196\\Documents\\extsort\\java-external-sorting-master\\data\\input1G.txt";
//        ExternalSort.main(new String[]{s, "output.txt"}); // check that it does not crash
//    }

//  22min，有待提升
//    @Test
//    public void testMain8G() throws Exception {
//        String s = "C:\\Users\\50196\\Documents\\extsort\\java-external-sorting-master\\data\\input.txt";
//        ExternalSort.main(new String[]{s, "output.txt"}); // check that it does not crash
//    }

}
