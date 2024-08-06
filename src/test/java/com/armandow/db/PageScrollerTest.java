package com.armandow.db;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class PageScrollerTest {
    @Test
    void testCreate() {
        for (int i = 1; i <= 35; i++) {
            var pageScroller = PageScroller.create(12, 1734, 50, i);
            assertNotNull(pageScroller);
            log.trace("{} :: {}", pageScroller.size(), pageScroller);
            assertTrue(pageScroller.size() >=12 && pageScroller.size() <= 13);

            var pageScroller2 = PageScroller.create(7, 3, 10, i);
            assertNotNull(pageScroller2);
            log.trace("{} :: {}", pageScroller2.size(), pageScroller2);
            assertTrue(pageScroller2.size() <= 1);
        }
    }
}
