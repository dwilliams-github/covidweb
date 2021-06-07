# AWS deployment

These are instructions for a manual deployment on an AWS EC2 instance. There 
are probably better ways of doing this in terms of continuous integration
and elastic resource allocation, but this gets the job done for this 
simple web site.

I choose an Ubuntu 20.04 machine image. An Amazon or RHEL image would work
fine, but the setup instructions will be different.

## Instance type

This web site uses a bit of caching in redis. You need enough memory
to hold all the various data tables that might be invoked. For that
reason at least a t2.medium instance or better is probably a good idea.
Disk space should not be an issue.

## Packages

We need nginx, gunicorn, redis, and various Python packages. We'll
also add certbot for free ssl.

```
# add-apt-repository ppa:certbot/certbot
# apt-get update
# apt-get install nginx redis gunicorn python3-pip python3-certbot-nginx
# pip3 install flask altair redis pyarrow
```

## Code

I put the contents of this package under ```/srv/covidweb```. You don't
need the ```.git``` subdirectory.

## Config files

The following is a gunicorn service, placed in ```/etc/systemd/system```,
and the corresponding nginx configuration, placed in ```/etc/nginx/sites-available```.
Note that certbot will later modify the nginx configuration.

### /etc/systemd/system/gunicorn.service

```
[Unit]
Description=Gunicorn service
After=network.target

[Service]
User=ubuntu
Group=www-data
WorkingDirectory=/srv/covidweb
RuntimeDirectory=nginx
ExecStart=/usr/bin/gunicorn3 --workers 3 --bind unix:/run/nginx/flask.sock -m 007 server:app

[Install]
WantedBy=multi-user.target
```

### /etc/nginx/sites-available/flask

```
server {
    listen 80;
    server_name covid19.slashdave.com;

    location / {
        proxy_pass http://unix:/run/nginx/flask.sock;
    }
}
```

## Setup


```
# systemctl enable redis
# systemctl start redis
```

```
# systemctl enable gunicorn
# systemctl start gunicorn
```

```
# cd /etc/nginx/sites-enabled
# rm default
# ln -s ../sites-available/flask .
# systemctl restart nginx
```

Note that the web server needs to be up before we can make our cert.

```
# certbot --nginx -d covid19.slashdave.com
```

This will also install a cron job to keep the cert up to date.
