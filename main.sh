current_date = $(date)

git pull
python get_day_game_stats.py
python combine.py
git add .
git commit -m "${current_date} Data Update"
git push