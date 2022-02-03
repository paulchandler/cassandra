package org.apache.cassandra.repair.consistent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class BulkRepairStateTest {
	   private static Token tk(long t)
	    {
	        return new Murmur3Partitioner.LongToken(t);
	    }

	    private static Range<Token> range(long left, long right)
	    {
	        return new Range<>(tk(left), tk(right));
	    }

	    private static List<Range<Token>> ranges(long... tokens)
	    {
	        assert tokens.length %2 == 0;
	        List<Range<Token>> ranges = new ArrayList<>();
	        for (int i=0; i<tokens.length; i+=2)
	        {
	            ranges.add(range(tokens[i], tokens[i+1]));

	        }
	        return ranges;
	    }

	    private static RepairedState.Level level(Collection<Range<Token>> ranges, long repairedAt)
	    {
	        return new RepairedState.Level(ranges, repairedAt);
	    }

	    private static RepairedState.Section sect(Range<Token> range, long repairedAt)
	    {
	        return new RepairedState.Section(range, repairedAt);
	    }

	    private static RepairedState.Section sect(int l, int r, long time)
	    {
	        return sect(range(l, r), time);
	    }

	    private static <T> List<T> l(T... contents)
	    {
	        return Lists.newArrayList(contents);
	    }

	    @Test
	    public void mergeOverlapping()
	    {
	        RepairedState repairs = new RepairedState();

	        repairs.storeInitialLevel(ranges(100, 300), 5);
	        repairs.storeInitialLevel(ranges(200, 400), 6);
	        repairs.finaliseInitalLevels();

	        RepairedState.State state = repairs.state();
	        Assert.assertEquals(l(level(ranges(200, 400), 6), level(ranges(100, 200), 5)), state.levels);
	        Assert.assertEquals(l(sect(range(100, 200), 5), sect(range(200, 400), 6)), state.sections);
	        Assert.assertEquals(ranges(100, 400), state.covered);
	    }

	    @Test
	    public void mergeSameRange()
	    {
	        RepairedState repairs = new RepairedState();

	        repairs.storeInitialLevel(ranges(100, 400), 5);
	        repairs.storeInitialLevel(ranges(100, 400), 6);
	        repairs.finaliseInitalLevels();

	        RepairedState.State state = repairs.state();
	        Assert.assertEquals(l(level(ranges(100, 400), 6)), state.levels);
	        Assert.assertEquals(l(sect(range(100, 400), 6)), state.sections);
	        Assert.assertEquals(ranges(100, 400), state.covered);
	    }

	    @Test
	    public void mergeLargeRange()
	    {
	        RepairedState repairs = new RepairedState();

	        repairs.storeInitialLevel(ranges(200, 300), 5);
	        repairs.storeInitialLevel(ranges(100, 400), 6);
	        repairs.finaliseInitalLevels();

	        RepairedState.State state = repairs.state();
	        Assert.assertEquals(l(level(ranges(100, 400), 6)), state.levels);
	        Assert.assertEquals(l(sect(range(100, 400), 6)), state.sections);
	        Assert.assertEquals(ranges(100, 400), state.covered);
	    }

	    @Test
	    public void mergeSmallRange()
	    {
	        RepairedState repairs = new RepairedState();

	        repairs.storeInitialLevel(ranges(100, 400), 5);
	        repairs.storeInitialLevel(ranges(200, 300), 6);
	        repairs.finaliseInitalLevels();

	        RepairedState.State state = repairs.state();
	        Assert.assertEquals(l(level(ranges(200, 300), 6), level(ranges(100, 200, 300, 400), 5)), state.levels);
	        Assert.assertEquals(l(sect(range(100, 200), 5), sect(range(200, 300), 6), sect(range(300, 400), 5)), state.sections);
	        Assert.assertEquals(ranges(100, 400), state.covered);
	    }


	    @Test
	    public void repairedAt()
	    {
	        RepairedState rs;

	        // overlapping
	        rs = new RepairedState();
	        rs.storeInitialLevel(ranges(100, 300), 5);
	        rs.storeInitialLevel(ranges(200, 400), 6);
	        rs.finaliseInitalLevels();

	        Assert.assertEquals(5, rs.minRepairedAt(ranges(150, 250)));
	        Assert.assertEquals(5, rs.minRepairedAt(ranges(150, 160)));
	        Assert.assertEquals(5, rs.minRepairedAt(ranges(100, 200)));
	        Assert.assertEquals(6, rs.minRepairedAt(ranges(200, 400)));
	        Assert.assertEquals(0, rs.minRepairedAt(ranges(200, 401)));
	        Assert.assertEquals(0, rs.minRepairedAt(ranges(99, 200)));
	        Assert.assertEquals(0, rs.minRepairedAt(ranges(50, 450)));
	        Assert.assertEquals(0, rs.minRepairedAt(ranges(50, 60)));
	        Assert.assertEquals(0, rs.minRepairedAt(ranges(450, 460)));
	    }


}
