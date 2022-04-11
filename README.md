# zed-danshan-scripts

pm2 start npm  --name "bt_race_horses" -- run zed_horses_racing_update_odds  
pm2 start npm  --name "live_api" -- run live_api_cache_runner  
pm2 start npm  --name "studs_api" -- run studs_api_cache_runner  
pm2 start npm  --name "new_horses" -- run zed_horses_needed_manual_using_api  
pm2 start npm  --name "unnamedfoal" -- run zed_horses_fix_unnamed_foal_cron  
pm2 start npm  --name "bt_tour_cron" -- run zed_tour_cron  
pm2 start npm  --name "bt_tour_leader_cron" -- run zed_tour_leader_cron  
pm2 start npm  --name "bt_tour_missed_cron" -- run zed_tour_missed_cron  
pm2 start npm --name "leader_b2_cron" -- run leaderboard_b2_cron  
pm2 start npm --name "cmap_combs" -- run get_parents_color_pair_chart_cron  

compiler
pm2 start node --name "comp_dp" -- code/main.js --compiler_dp run_cron  
pm2 start node --name "comp_ba" -- code/main.js --compiler_ba run_cron  
pm2 start node --name "comp_rng" -- code/main.js --compiler_rng run_cron  

races 
pm2 start node  --name "bt_races_live" -- code/main --races live_cron  
pm2 start node  --name "bt_races_miss" -- code/main --races miss_cron  
pm2 start node  --name "bt_races_scheduled" -- code/main --races scheduled_cron  
pm2 start node  --name "bt_races_duplicate" -- code/main --races duplicate_cron  

pm2 start node  --name "bt_horses_fix" -- code/main.js --horses fixer
pm2 start node  --name "bt_horses_new" -- code/main.js --horses new

pm2 start node --name "line_run_cron" -- code/main.js v5 --line run_cron
pm2 start node --name "tourney_run_cron" -- code/main.js v5 --tourney run_cron