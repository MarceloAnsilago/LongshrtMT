# Fly.io deploy notes

## Secrets

```bash
fly secrets set DJANGO_ALLOWED_HOSTS=".fly.dev,longshort-django-mt5.fly.dev,localhost,127.0.0.1" -a longshort-django-mt5
fly secrets set DJANGO_CSRF_TRUSTED_ORIGINS="https://longshort-django-mt5.fly.dev" -a longshort-django-mt5
fly deploy -a longshort-django-mt5
```

## Why the 400 after login

The 400 on POST /accounts/login was caused by missing proxy HTTPS headers handling
(USE_X_FORWARDED_HOST / SECURE_PROXY_SSL_HEADER) and/or overly restrictive
ALLOWED_HOSTS and CSRF_TRUSTED_ORIGINS when running behind Fly's HTTPS proxy.
