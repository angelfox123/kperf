package Util;

import static org.junit.Assert.*;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class BusyTimerTest {

    @Test
    public void scheduleTaskBaseFirstMessageTimeTest() {
        long interval = 100;
        long duration = 3;
        BusyTimer timer = new BusyTimer(interval, duration, new Runnable() {
            @Override
            public void run() {
                //do nothing
            }
        });
        long base = timer.scheduleTaskBaseFirstMessageTime();

        for(int i=2; i< duration * 1000 * 1000 / interval; i++){
            System.out.println(timer.pubTime.get(i) - (base + interval * i * 1000));
            Assert.assertTrue( timer.pubTime.get(i) - (base + interval * i * 1000) < 10000);
        }
    }

    @Test
    public void spaceMessageWithIntervalTest(){
        long interval =100;
        long duration = 3;
        BusyTimer timer = new BusyTimer(interval, duration, new Runnable() {
            @Override
            public void run() {
                //do nothing
            }
        });
        timer.spaceMessageWithInterval();
        for(int i = 1; i < timer.pubTime.size(); i++){
            System.out.println(timer.pubTime.get(i) - timer.pubTime.get(i-1) - interval * 1000);
            Assert.assertTrue(timer.pubTime.get(i) - timer.pubTime.get(i-1) > interval * 1000 );
        }

    }
}