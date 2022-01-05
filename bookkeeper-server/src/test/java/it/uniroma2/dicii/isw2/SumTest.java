package it.uniroma2.dicii.isw2;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SumTest {
    private int firstAddend;
    private int secondAddend;
    private int expected;

    public SumTest(int firstAddend, int secondAddend, int expected) {
        this.firstAddend = firstAddend;
        this.secondAddend = secondAddend;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection params() {
        return Arrays.asList(new Object[][]{
                { 2, 0, 2 },
                { 6, 1, 7 },
                { -19, 21, 2 },
                { -3, 3, 0 },
                { -3, 2, -1 }
        });
    }

    @Test
    public void testSum() {
        assertEquals(expected,  firstAddend + secondAddend);
    }

}
