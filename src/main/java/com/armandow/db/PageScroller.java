package com.armandow.db;

import java.util.ArrayList;
import java.util.List;

public class PageScroller {
    private PageScroller() {
        // PageScroller
    }

    public static List<Integer> create(Integer maxScrollElements, Integer dataSize, Integer pageSize, Integer currentPage) {
        var pageScroller = new ArrayList<Integer>();
        int totalPages;
        int i;

        if (maxScrollElements % 2 == 0) {
            maxScrollElements++;
        }

        totalPages = (dataSize / pageSize) + ((dataSize % pageSize != 0) ? 1 : 0);

        if (totalPages <= maxScrollElements) {
            for (i = 0; i < totalPages; i++) {
                pageScroller.add(i + 1);
            }

            return pageScroller;
        }

        if (currentPage < (maxScrollElements - 4)) {
            for (i = 0; i < (maxScrollElements - 2); i++) {
                pageScroller.add(i + 1);
            }

            pageScroller.add(null);
            pageScroller.add(totalPages);

            return pageScroller;
        }

        if (((totalPages - currentPage) + 1) < (maxScrollElements - 4)) {
            pageScroller.add(1);
            pageScroller.add(null);

            for (i = (totalPages - (maxScrollElements - 3)); i < totalPages; i++) {
                pageScroller.add(i + 1);
            }

            return pageScroller;
        }

        pageScroller.add(1);
        pageScroller.add(null);

        for (i = 0; i < (maxScrollElements - 4); i++) {
            pageScroller.add(i + (currentPage - ((maxScrollElements - 4) / 2)));
        }

        pageScroller.add(null);
        pageScroller.add(totalPages);

        return pageScroller;
    }
}
