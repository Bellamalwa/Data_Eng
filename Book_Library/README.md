### To automate 
A tool built in Mac (and all Linux servers) called Cron. It acts like a invisible alarm clock that stays awake even when you aren't looking at your computer.

Here is the 3-step summary of how to make it happen:

1. ### The "Command Center" (crontab)

We accessed the "Schedule Sheet" by typing crontab -e in the Terminal. This opened a private file where the computer looks for instructions every single minute of the day.

2. ### The "Secret Code" (The Timing)

We wrote a specific string of numbers and stars that the computer translates into a schedule: 0 9 * * *

0: Start at exactly minute zero.

9: Start at the 9th hour (9:00 AM).

*: Run every day of the month.

*: Run every month.

*: Run every day of the week.

3. ### The "Instruction Line"

After the timing code, we gave the computer the Exact Map to your factory. Because the "Alarm Clock" doesn't know where you keep your files, we had to use "Absolute Paths" (starting from the very root of your hard drive).

The full line looked like this: 0 9 * * * /usr/bin/python3 /Users/bella/library_v2.py >> /Users/bella/factory_log.txt 2>&1

Why we added the extra symbols at the end:

>>: This is a funnel. It tells the colsmputer: "Don't show the results on the screen; save them into the factory_log.txt file instead."

2>&1: This is a safety catch. It tells the computer: "If the script crashes, write the error message into the log file too so I can read it later."
o run your factory twice a day—for example, at 9:00 AM and 9:00 PM—you have two ways to do it.

Option A: The "Comma" Shortcut

Instead of writing two separate lines, you can tell the hour column to trigger at two different times using a comma.

The Code: 0 9,21 * * *

9: 9:00 AM

21: 9:00 PM (Cron uses a 24-hour clock)

Option B: The "Step" Shortcut (Every X Hours)

If you wanted your factory to run every 12 hours (regardless of the specific time), you use a forward slash.

The Code: 0 */12 * * *

0: At the start of the hour.

*/12: Every 12th hour (12:00 AM and 12:00 PM).c

 to check content of the factory file cat /Users/bella/factory_log.txt