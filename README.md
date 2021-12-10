# zed-danshan-scripts

pm2 start npm  --name "bt_races" -- run zed_races_automated_script_run
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