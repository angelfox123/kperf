package Util;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BusyTimer {
    long interval;
    long duration;
    ArrayList<Long> pubTime;
    ExecutorService ex = Executors.newSingleThreadExecutor();
    Runnable task;


    public BusyTimer(long microInterval, long exDurationInSeconds, Runnable task){
        pubTime = new ArrayList<Long>((int)(exDurationInSeconds * 1000 * 1000 / microInterval+1));

        this.interval = microInterval * 1000;
        this.duration = exDurationInSeconds * 1000000000;
        this.task = task;

    }

    private void busywaitUntil(long nano){
        while(System.nanoTime() < nano){

        }
    }

    public void spaceMessageWithInterval(){
        int i =0 ;
        long baseTime = System.nanoTime();
        long doneTime = baseTime + duration;
        while(true) {
            task.run();
            pubTime.add(System.nanoTime());
            long targetTime = System.nanoTime() + interval;
            if(System.nanoTime() > doneTime ){
                break;
            }
            busywaitUntil(targetTime);

        }
    }

    public long scheduleTaskBaseFirstMessageTime(){

        int i =0 ;
        long baseTime = System.nanoTime();
        long doneTime = baseTime + duration;
        while(true){

            busywaitUntil(interval * i + baseTime );
            pubTime.add(System.nanoTime());
            //ex.submit(task);
            task.run();
            i++;
            if(System.nanoTime() > doneTime ){
                break;
            }

        }
        return baseTime;
    }


}
