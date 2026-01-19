source env/bin/activate
caffeinate -i -s python3 hedge_mode.py --exchange backpack --ticker FOGO --size 4800 --iter 8 --sleep 3600

python3 dashboard_server.py

source env/bin/activate
caffeinate -i -s python3 hedge_mode.py --exchange backpack --ticker HBAR --size 1500 --iter 6 --sleep 3618

python3 dashboard_server.py
