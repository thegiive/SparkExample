cd /root; openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout mycert.pem -out mycert.pem
ipython profile create default
python -c "from IPython.lib import passwd; print passwd()" > /root/.ipython/profile_default/nbpasswd.txt

