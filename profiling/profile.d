#pragma D option quiet

pid$target::*surreal*:entry
{
    self->entry_time = timestamp;
    self->v_entry_time = vtimestamp;
}


pid$target::*surreal*:return
{
    total_time = timestamp - self->entry_time;
    cpu_time = vtimestamp - self->v_entry_time;

    @counts[probefunc] = count();
    @sum_total_times[probefunc] = sum(total_time);
    @sum_cpu_times[probefunc] = sum(cpu_time);
    @average_total_times[probefunc] = avg(total_time);
    @average_cpu_times[probefunc] = avg(cpu_time);

    self->entry_time = 0;
    self->v_entry_time = 0;
}

dtrace:::END
{
    printa("%s %@d %@d %@d %@d %@d\n",
        @average_cpu_times,
        @sum_cpu_times,
        @average_total_times,
        @sum_total_times,
        @counts);
}