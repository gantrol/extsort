package com.gantrol.extsort;

import java.io.IOException;

public interface StreamStack {
    void close() throws IOException;

    boolean empty();

    int peek();

    int pop() throws IOException;
}
