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

race_horses
pm2 start node --name "bt3_race_horses" -- code/main.js --race_horses run_cron
pm2 start node --name "bt3_race_horses_miss" -- code/main.js --race_horses run_miss_cron

horses 
pm2 start node  --name "bt_horses_fix" -- code/main.js --horses fixer 
pm2 start node  --name "bt_horses_new" -- code/main.js --horses new 

pm2 start node --name "line_run_cron" -- code/main.js v5 --line run_cron 

tourney 
pm2 start node --name "tourney::run_cron" -- code/main.js --tourney run_cron 
pm2 start node --name "tourney::run_flash_cron" -- code/main.js --tourney run_flash_cron 
pm2 start node --name "elo_h_getter_run_cron" -- code/main.js --tourney elo_h_getter_run_cron 

ymca5_table 
pm2 start node --name "ymca5_table_cron" -- code/main.js --ymca5_table run_cron 

payments 
pm2 start node --name "payments_cron" -- code/main.js --payments run_cron 

parents htype 
pm2 start node --name "parents_htype " -- code/main.js --parents fix_horse_type_all_cron

hawku prices 
pm2 start node --name "hawku_prices" -- code/main.js --hawku run_cron 
pm2 start node --name "hawku_prices_fixer" -- code/main.js --hawku fixer_cron 

mate 
pm2 start node --name "mate::cron" -- code/main.js --mate run_cron 

fixed unnamed cron
pm2 start node --name "fix_unnamed" -- code/main.js --horses fix_unnamed_cron

tqual 
pm2 start node --name "tqual::run_cron" -- code/main.js --tqual cron 

stables
pm2 start node --name "stables getter" --  code/main.js --stables all

sn_pro 
pm2 start node --name "sn_pro:cron_txns" -- code/main.js --sn_pro run_cron_txns 
pm2 start node --name "sn_pro:cron_stables" -- code/main.js --sn_pro run_cron_stables 