==========================================
## Variale Naming Strategy
==========================================

1) mx = 85000; => maximim number of zed horses that are there

2) st = 0; => horses start from 0

3) ed = mx; => horses end at maximum

4) chunk_size
chunk_size is the number of horses we want to fetch all together
chunk_size=25 means we fetch bunches of 25 25 horses each time
increasing chunk_size will fasten our script 
beware if you increase it too much api.zed start giving errors 

5) chunk_delay
chunk_delay is the delay after each chunk fetch
increasing delay slows our script but also reduces errors from api.zed
----------------------------------------------------------------------------------------------

==========================================
## Script with code file association
==========================================
codes folder => js_code_files

R1_zed_races_flames_and_odds.sh => zed_races_flames.js

R2_update_horses_odds.sh => index-odds-generator.js

R3_update_breed_scores.sh => horses-kids.js
----------------------------------------------------------------------------------------------


==========================================
## What each js code file is for 
==========================================
codes folder => js_code_files

=) base.js
this file is to get and update ETH prices on local
dont need to seperately run it or worry
its automatically run for our scripts
and for too automatically updates at 00:10am UTC time

=) horses-kids.js
this file is for breed_score  

=) index-odds-generator.js
this file is for horses odds overall, live, blood, and ranking of horse

=) index-run.js
this file is to connect to our mongo databases

=) leaderboard-generator.js
this file is for our leaderboard

=) tests.js
this file is for our tests and temorary one time functions 
no association to scripts1

=) utils.js
this file contails all uitlity functions that most of our code requrires at many places

=) zed_races_avging.js
this file was to calculate the position_flame odds after zed removed odds from further races after 25th October

=) zed_races_flames.js
this file is code to update flames and odds of upcoming races 
----------------------------------------------------------------------------------------------
