package com.gantrol.extsort;

import java.io.BufferedReader;
import java.io.IOException;

public class BinaryFileBuffer implements StreamStack {
    private final BufferedReader fbr;
    private Integer cache;

    public BinaryFileBuffer(BufferedReader r) throws IOException {
        this.fbr = r;
        reload();
    }

    public void close() throws IOException {
        this.fbr.close();
    }

    public boolean empty() {
        return this.cache == null;
    }

    public int peek() {
        return this.cache;
    }

    @Override
    public int pop() throws IOException {
        int answer = peek();// make a copy
        reload();
        return answer;
    }

    private void reload() throws IOException {
        String line = this.fbr.readLine();
        if (line == null) {
            this.cache = null;
        } else {
            this.cache = Integer.parseInt(line);
        }

    }
}
