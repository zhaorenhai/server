/* Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2019, 2020 MariaDB Corporation.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1335  USA */

/*
  rdtsc3 -- multi-platform timer code
  pgulutzan@mysql.com, 2005-08-29
  modified 2008-11-02

  Functions:

  my_timer_cycles           ulonglong cycles
  my_timer_nanoseconds      ulonglong nanoseconds
  my_timer_microseconds     ulonglong "microseconds"
  my_timer_milliseconds     ulonglong milliseconds
  my_timer_ticks            ulonglong ticks
  my_timer_init             initialization / test

  We'll call the first 5 functions (the ones that return
  a ulonglong) "my_timer_xxx" functions.
  Each my_timer_xxx function returns a 64-bit timing value
  since an arbitrary 'epoch' start. Since the only purpose
  is to determine elapsed times, wall-clock time-of-day
  is not known and not relevant.

  The my_timer_init function is necessary for initializing.
  It returns information (underlying routine name,
  frequency, resolution, overhead) about all my_timer_xxx
  functions. A program should call my_timer_init once,
  use the information to decide what my_timer_xxx function
  to use, and subsequently call that function by function
  pointer.

  A typical use would be:
  my_timer_init()        ... once, at program start
  ...
  time1= my_timer_xxx()  ... time before start
  [code that's timed]
  time2= my_timer_xxx()  ... time after end
  elapsed_time= (time2 - time1) - overhead
*/

#include "my_global.h"
#include "my_rdtsc.h"

#include <chrono>

#if defined(_WIN32)
#include "windows.h"
#endif

#if defined(HAVE_SYS_TIMES_H) && defined(HAVE_TIMES)
#include <sys/times.h>       /* for times */
#endif

using Clock= std::chrono::system_clock;

ulonglong my_timer_nanoseconds(void)
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             Clock::now().time_since_epoch())
      .count();
}

ulonglong my_timer_microseconds(void)
{
  return std::chrono::duration_cast<std::chrono::microseconds>(
             Clock::now().time_since_epoch())
      .count();
}

ulonglong my_timer_milliseconds(void)
{
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             Clock::now().time_since_epoch())
      .count();
}

/*
  For ticks, which we handle with times(), the frequency
  is usually 100/second and the overhead is surprisingly
  bad, sometimes even worse than gettimeofday's overhead.
*/

ulonglong my_timer_ticks(void)
{
#if defined(HAVE_SYS_TIMES_H) && defined(HAVE_TIMES)
  {
    struct tms times_buf;
    return (ulonglong) times(&times_buf);
  }
#elif defined(_WIN32)
  return (ulonglong) GetTickCount();
#else
  return 0;
#endif
}

/*
  The my_timer_init() function and its sub-functions
  have several loops which call timers. If there's
  something wrong with a timer -- which has never
  happened in tests -- we want the loop to end after
  an arbitrary number of iterations, and my_timer_info
  will show a discouraging result. The arbitrary
  number is 1,000,000.
*/
#define MY_TIMER_ITERATIONS 1000000

/*
  Calculate overhead. Called from my_timer_init().
  Usually best_timer_overhead = cycles.overhead or
  nanoseconds.overhead, so returned amount is in
  cycles or nanoseconds. We repeat the calculation
  ten times, so that we can disregard effects of
  caching or interrupts. Result is quite consistent
  for cycles, at least. But remember it's a minimum.
*/

static void my_timer_init_overhead(ulonglong *overhead,
                                   ulonglong (*cycle_timer)(void),
                                   ulonglong (*this_timer)(void),
                                   ulonglong best_timer_overhead)
{
  ulonglong time1, time2;
  int i;

  /* *overhead, least of 20 calculations - cycles.overhead */
  for (i= 0, *overhead= 1000000000; i < 20; ++i)
  {
    time1= cycle_timer();
    this_timer(); /* rather than 'time_tmp= timer();' */
    time2= cycle_timer() - time1;
    if (*overhead > time2)
      *overhead= time2;
  }
  *overhead-= best_timer_overhead;
}

/*
  Calculate Resolution. Called from my_timer_init().
  If a timer goes up by jumps, e.g. 1050, 1075, 1100, ...
  then the best resolution is the minimum jump, e.g. 25.
  If it's always divisible by 1000 then it's just a
  result of multiplication of a lower-precision timer
  result, e.g. nanoseconds are often microseconds * 1000.
  If the minimum jump is less than an arbitrary passed
  figure (a guess based on maximum overhead * 2), ignore.
  Usually we end up with nanoseconds = 1 because it's too
  hard to detect anything <= 100 nanoseconds.
  Often GetTickCount() has resolution = 15.
  We don't check with ticks because they take too long.
*/
static ulonglong my_timer_init_resolution(ulonglong (*this_timer)(void),
                                          ulonglong overhead_times_2)
{
  ulonglong time1, time2;
  ulonglong best_jump;
  int i, jumps, divisible_by_1000, divisible_by_1000000;

  divisible_by_1000= divisible_by_1000000= 0;
  best_jump= 1000000;
  for (i= jumps= 0; jumps < 3 && i < MY_TIMER_ITERATIONS * 10; ++i)
  {
    time1= this_timer();
    time2= this_timer();
    time2-= time1;
    if (time2)
    {
      ++jumps;
      if (!(time2 % 1000))
      {
        ++divisible_by_1000;
        if (!(time2 % 1000000))
          ++divisible_by_1000000;
      }
      if (best_jump > time2)
        best_jump= time2;
      /* For milliseconds, one jump is enough. */
      if (overhead_times_2 == 0)
        break;
    }
  }
  if (jumps == 3)
  {
    if (jumps == divisible_by_1000000)
      return 1000000;
    if (jumps == divisible_by_1000)
      return 1000;
  }
  if (best_jump > overhead_times_2)
    return best_jump;
  return 1;
}

/*
  Calculate cycle frequency by seeing how many cycles pass
  in a 200-microsecond period. I tried with 10-microsecond
  periods originally, and the result was often very wrong.
*/

static ulonglong my_timer_init_frequency(MY_TIMER_INFO *mti)
{
  int i;
  ulonglong time1, time2, time3, time4;
  time1= my_timer_cycles();
  time2= my_timer_microseconds();
  time3= time2; /* Avoids a Microsoft/IBM compiler warning */
  for (i= 0; i < MY_TIMER_ITERATIONS; ++i)
  {
    time3= my_timer_microseconds();
    if (time3 - time2 > 200) break;
  }
  time4= my_timer_cycles() - mti->cycles.overhead;
  time4-= mti->microseconds.overhead;
  return (mti->microseconds.frequency * (time4 - time1)) / (time3 - time2);
}

/*
  Call my_timer_init before the first call to my_timer_xxx().
  If something must be initialized, it happens here.
  Set: what routine is being used e.g. "rdtsc"
  Set: function, overhead, actual frequency, resolution.
*/

void my_timer_init(MY_TIMER_INFO *mti)
{
  ulonglong (*best_timer)(void);
  ulonglong best_timer_overhead;
  ulonglong time1, time2;
  int i;

  /* cycles */
  mti->cycles.frequency= 1000000000;
#if defined _WIN32 || defined __i386__ || defined __x86_64__
  mti->cycles.routine= MY_TIMER_ROUTINE_RDTSC;
#elif defined(__INTEL_COMPILER) && defined(__ia64__) && defined(HAVE_IA64INTRIN_H)
  mti->cycles.routine= MY_TIMER_ROUTINE_ASM_IA64;
#elif defined(__GNUC__) && defined(__ia64__)
  mti->cycles.routine= MY_TIMER_ROUTINE_ASM_IA64;
#elif defined __GNUC__ && defined __powerpc__
  mti->cycles.routine= MY_TIMER_ROUTINE_PPC_GET_TIMEBASE;
#elif defined(__GNUC__) && defined(__sparcv9) && defined(_LP64) && (__GNUC__>2)
  mti->cycles.routine= MY_TIMER_ROUTINE_ASM_GCC_SPARC64;
#elif defined(__GNUC__) && defined(__sparc__) && !defined(_LP64) && (__GNUC__>2)
  mti->cycles.routine= MY_TIMER_ROUTINE_ASM_GCC_SPARC32;
#elif defined(__GNUC__) && defined(__s390__)
  mti->cycles.routine= MY_TIMER_ROUTINE_ASM_S390;
#elif defined(HAVE_SYS_TIMES_H) && defined(HAVE_GETHRTIME)
  mti->cycles.routine= MY_TIMER_ROUTINE_GETHRTIME;
#else
  mti->cycles.routine= 0;
#endif

  if (!mti->cycles.routine || !my_timer_cycles())
  {
    mti->cycles.routine= 0;
    mti->cycles.resolution= 0;
    mti->cycles.frequency= 0;
    mti->cycles.overhead= 0;
  }

  /* nanoseconds */
  mti->nanoseconds.frequency=  1000000000; /* initial assumption */
  mti->nanoseconds.routine= MY_TIMER_ROUTINE_STD_CHRONO;

  if (!mti->nanoseconds.routine || !my_timer_nanoseconds())
  {
    mti->nanoseconds.routine= 0;
    mti->nanoseconds.resolution= 0;
    mti->nanoseconds.frequency= 0;
    mti->nanoseconds.overhead= 0;
  }

  /* microseconds */
  mti->microseconds.frequency= 1000000; /* initial assumption */
  mti->microseconds.routine= MY_TIMER_ROUTINE_STD_CHRONO;

  if (!mti->microseconds.routine || !my_timer_microseconds())
  {
    mti->microseconds.routine= 0;
    mti->microseconds.resolution= 0;
    mti->microseconds.frequency= 0;
    mti->microseconds.overhead= 0;
  }

  /* milliseconds */
  mti->milliseconds.frequency= 1000; /* initial assumption */
  mti->milliseconds.routine= MY_TIMER_ROUTINE_STD_CHRONO;

  if (!mti->milliseconds.routine || !my_timer_milliseconds())
  {
    mti->milliseconds.routine= 0;
    mti->milliseconds.resolution= 0;
    mti->milliseconds.frequency= 0;
    mti->milliseconds.overhead= 0;
  }

  /* ticks */
  mti->ticks.frequency= 100; /* permanent assumption */
#if defined(HAVE_SYS_TIMES_H) && defined(HAVE_TIMES)
  mti->ticks.routine= MY_TIMER_ROUTINE_TIMES;
#elif defined(_WIN32)
  mti->ticks.routine= MY_TIMER_ROUTINE_GETTICKCOUNT;
#else
  mti->ticks.routine= 0;
#endif
  if (!mti->ticks.routine || !my_timer_ticks())
  {
    mti->ticks.routine= 0;
    mti->ticks.resolution= 0;
    mti->ticks.frequency= 0;
    mti->ticks.overhead= 0;
  }

  /*
    Calculate overhead in terms of the timer that
    gives the best resolution: cycles or nanoseconds.
    I doubt it ever will be as bad as microseconds.
  */
  if (mti->cycles.routine)
    best_timer= &my_timer_cycles;
  else
    best_timer= &my_timer_nanoseconds;

  /* best_timer_overhead = least of 20 calculations */
  for (i= 0, best_timer_overhead= 1000000000; i < 20; ++i)
  {
    time1= best_timer();
    time2= best_timer() - time1;
    if (best_timer_overhead > time2)
      best_timer_overhead= time2;
  }
  if (mti->cycles.routine)
    my_timer_init_overhead(&mti->cycles.overhead,
                           best_timer,
                           &my_timer_cycles,
                           best_timer_overhead);
  if (mti->nanoseconds.routine)
    my_timer_init_overhead(&mti->nanoseconds.overhead,
                           best_timer,
                           &my_timer_nanoseconds,
                           best_timer_overhead);
  if (mti->microseconds.routine)
    my_timer_init_overhead(&mti->microseconds.overhead,
                           best_timer,
                           &my_timer_microseconds,
                           best_timer_overhead);
  if (mti->milliseconds.routine)
    my_timer_init_overhead(&mti->milliseconds.overhead,
                           best_timer,
                           &my_timer_milliseconds,
                           best_timer_overhead);
  if (mti->ticks.routine)
    my_timer_init_overhead(&mti->ticks.overhead,
                           best_timer,
                           &my_timer_ticks,
                           best_timer_overhead);

  /*
    Calculate resolution for nanoseconds or microseconds
    or milliseconds, by seeing if it's always divisible
    by 1000, and by noticing how much jumping occurs.
    For ticks, just assume the resolution is 1.
  */
  if (mti->cycles.routine)
    mti->cycles.resolution= 1;
  mti->nanoseconds.resolution=
      my_timer_init_resolution(&my_timer_nanoseconds, 20000);
  mti->microseconds.resolution=
      my_timer_init_resolution(&my_timer_microseconds, 20);
  mti->milliseconds.resolution=
      my_timer_init_resolution(&my_timer_milliseconds, 0);
  if (mti->ticks.routine)
    mti->ticks.resolution= 1;

/*
  Calculate cycles frequency, if we have a cycles routine*/
  if (mti->cycles.routine)
  {
    time1= my_timer_init_frequency(mti);
    /* Repeat once in case there was an interruption. */
    time2= my_timer_init_frequency(mti);
    if (time1 < time2)
      mti->cycles.frequency= time1;
    else
      mti->cycles.frequency= time2;
  }

/*
  Calculate milliseconds frequency =
  (cycles-frequency/#-of-cycles) * #-of-milliseconds,
  if we have a cycles routine.
  This will be inaccurate if milliseconds resolution > 1.
  This is probably only useful when testing new platforms.
*/
  if (mti->milliseconds.routine
  &&  mti->milliseconds.resolution < 1000
  &&  mti->cycles.routine)
  {
    ulonglong time3, time4;
    time1= my_timer_cycles();
    time2= my_timer_milliseconds();
    time3= time2; /* Avoids a Microsoft/IBM compiler warning */
    for (i= 0; i < MY_TIMER_ITERATIONS * 1000; ++i)
    {
      time3= my_timer_milliseconds();
      if (time3 - time2 > 10) break;
    }
    time4= my_timer_cycles();
    mti->milliseconds.frequency=
    (mti->cycles.frequency * (time3 - time2)) / (time4 - time1);
  }

/*
  Calculate ticks.frequency =
  (cycles-frequency/#-of-cycles * #-of-ticks,
  if we have both a cycles routine,
  This is probably only useful when testing new platforms.
*/
  if (mti->ticks.routine
  &&  mti->cycles.routine)
  {
    ulonglong time3, time4;
    time1= my_timer_cycles();
    time2= my_timer_ticks();
    time3= time2; /* Avoids a Microsoft/IBM compiler warning */
    for (i= 0; i < MY_TIMER_ITERATIONS * 1000; ++i)
    {
      time3= my_timer_ticks();
      if (time3 - time2 > 10) break;
    }
    time4= my_timer_cycles();
    mti->ticks.frequency=
    (mti->cycles.frequency * (time3 - time2)) / (time4 - time1);
  }
}

/*
   Additional Comments
   -------------------

   This is for timing, i.e. finding out how long a piece of code
   takes. If you want time of day matching a wall clock, the
   my_timer_xxx functions won't help you.

   The best timer is the one with highest frequency, lowest
   overhead, and resolution=1. The my_timer_info() routine will tell
   you at runtime which timer that is. Usually it will be
   my_timer_cycles() but be aware that, although it's best,
   it has possible flaws and dangers. Depending on platform:
   - The frequency might change. We don't test for this. It
     happens on laptops for power saving, and on blade servers
     for avoiding overheating.
   - The overhead that my_timer_init() returns is the minimum.
     In fact it could be slightly greater because of caching or
     because you call the routine by address, as recommended.
     It could be hugely greater if there's an interrupt.
   - The x86 cycle counter, RDTSC doesn't "serialize". That is,
     if there is out-of-order execution, rdtsc might be processed
     after an instruction that logically follows it.
     (We could force serialization, but that would be slower.)
   - It is possible to set a flag which renders RDTSC
     inoperative. Somebody responsible for the kernel
     of the operating system would have to make this
     decision. For the platforms we've tested with, there's
     no such problem.
   - With a multi-processor arrangement, it's possible
     to get the cycle count from one processor in
     thread X, and the cycle count from another processor
     in thread Y. They may not always be in synch.
   - You can't depend on a cycle counter being available for
     all platforms. On Alphas, the
     cycle counter is only 32-bit, so it would overflow quickly,
     so we don't bother with it. On platforms that we haven't
     tested, there might be some if/endif combination that we
     didn't expect, or some assembler routine that we didn't
     supply.

   The recommended way to use the timer routines is:
   1. Somewhere near the beginning of the program, call
      my_timer_init(). This should only be necessary once,
      although you can call it again if you think that the
      frequency has changed.
   2. Determine the best timer based on frequency, resolution,
      overhead -- all things that my_timer_init() returns.
      Preserve the address of the timer and the my_timer_into
      results in an easily-accessible place.
   3. Instrument the code section that you're monitoring, thus:
      time1= my_timer_xxx();
      Instrumented code;
      time2= my_timer_xxx();
      elapsed_time= (time2 - time1) - overhead;
      If the timer is always on, then overhead is always there,
      so don't subtract it.
   4. Save the elapsed time, or add it to a totaller.
   5. When all timing processes are complete, transfer the
      saved / totalled elapsed time to permanent storage.
      Optionally you can convert cycles to microseconds at
      this point. (Don't do so every time you calculate
      elapsed_time! That would waste time and lose precision!)
      For converting cycles to microseconds, use the frequency
      that my_timer_init() returns. You'll also need to convert
      if the my_timer_microseconds() function is the Windows
      function QueryPerformanceCounter(), since that's sometimes
      a counter with precision slightly better than microseconds.

   Since we recommend calls by function pointer, we supply
   no inline functions.

   Some comments on the many candidate routines for timing ...

   __rdtsc(): available for IA-32 and AMD64.
   See "possible flaws and dangers" comments.

   times(): what we use for ticks. Should just read the last
   (xtime) tick count, therefore should be fast, but usually
   isn't.

   GetTickCount(): we use this for my_timer_ticks() on
   Windows. Actually it really is a tick counter, so resolution
   >= 10 milliseconds unless you have a very old Windows version.
   With Windows 95 or 98 or ME, timeGetTime() has better resolution than
   GetTickCount (1ms rather than 55ms). But with Windows NT or XP or 2000,
   they're both getting from a variable in the Process Environment Block
   (PEB), and the variable is set by the programmable interrupt timer, so
   the resolution is the same (usually 10-15 milliseconds). Also timeGetTime
   is slower on old machines:
   http://www.doumo.jp/aon-java/jsp/postgretips/tips.jsp?tips=74.
   Also timeGetTime requires linking winmm.lib,
   Therefore we use GetTickCount.
   It will overflow every 49 days because the return is 32-bit.
   There is also a GetTickCount64 but it requires Vista or Windows Server 2008.
   (As for GetSystemTimeAsFileTime, its precision is spurious, it
   just reads the tick variable like the other functions do.
   However, we don't expect it to overflow every 49 days, so we
   will prefer it for my_timer_milliseconds().)

   Any clock-based timer can be affected by NPT (ntpd program),
   which means:
   - full-second correction can occur for leap second
   - tiny corrections can occcur approimately every 11 minutes
     (but I think they only affect the RTC which isn't the PIT).

   We define "precision" as "frequency" and "high precision" is
   "frequency better than 1 microsecond". We define "resolution"
   as a synonym for "granularity". We define "accuracy" as
   "closeness to the truth" as established by some authoritative
   clock, but we can't measure accuracy.

   Do not expect any of our timers to be monotonic; we
   won't guarantee that they return constantly-increasing
   unique numbers.

*/

